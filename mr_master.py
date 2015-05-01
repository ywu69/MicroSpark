__author__ = 'pengzhan'
import sys

import zerorpc
import os
import gevent
import socket
from gevent import timeout

class Master(object):

    def __init__(self, data_dir):
        gevent.spawn(self.controller)
        self.state = 'READY'
        self.data_dir = data_dir

        self.workers = {}
        self.mapState = {}
        self.reduceState = {}
        self.reduce_id_list = {}

        self.chunkState = {}
        self.chunkWorker = {}
        self.ready_chunks_mappers = {}

    def controller(self):
        while True:
            print '[Master:%s] ' % (self.state),
            for w in self.workers:
                print '(%s,%s,%s)' % (w[0], w[1], self.mapState[w]),
            print
            for w in self.workers:
                if self.mapState[w] != "LOSS":
                    try:
                        self.workers[w].ping()
                    except Exception:
                        self.mapState[w] = "LOSS"
                        self.reduceState[w] = "LOSS"
                        print 'lost connection'
            gevent.sleep(1)

    def register_async(self, ip, port):
        print '[Master:%s] ' % self.state,
        print 'Registered worker (%s,%s)' % (ip, port)
        c = zerorpc.Client()
        c.connect("tcp://" + ip + ':' + port)
        self.workers[(ip,port)] = c
        self.mapState[(ip,port)] = 'READY'
        self.reduceState[(ip,port)] = 'READY'
        c.ping()

    def register(self, ip, port):
        gevent.spawn(self.register_async, ip, port)

    def set_worker_map_state(self, ip, port, state):
        gevent.spawn(self.set_worker_map_state_async, ip, port, state)

    def set_worker_map_state_async(self, ip, port, state):
        print 'set' +ip+':'+port+' map state ' + state
        self.mapState[(ip,port)] = state

    def set_worker_reduce_state(self, ip, port, state):
        gevent.spawn(self.set_worker_reduce_state_async, ip, port, state)

    def set_worker_reduce_state_async(self, ip, port, state):
        print 'set' +ip+':'+port+' reduce state ' + state
        self.reduceState[(ip,port)] = state

    def set_chunk_state(self, size, offset, state):
        gevent.spawn(self.set_chunk_state_async, size, offset, state)

    def set_chunk_state_async(self, size, offset, state):
        self.chunkState[(size, offset)] = state

    def write_reduce_result_list_to_file(self, w, input_filename, output_filename_base, reducer_id):
        try:
            temp_result_list = self.workers[w].get_result_list()
            f = open(os.getcwd()+'/'+self.data_dir+'/'+output_filename_base+'/'+output_filename_base + str(reducer_id)+'.txt', 'w')
            for e in temp_result_list:
                f.write(str(e[0])+':'+str(e[1])+'\n')
            f.close()
            self.workers[w].master_notice_received()
        except Exception:
            print 'Time out???'

    def set_job(self,job_name, split_size, num_reducers, input_filename, output_filename_base):
        gevent.spawn(self.setJob_async, job_name, split_size, num_reducers, input_filename, output_filename_base)

    def setJob_async(self,job_name, split_size, num_reducers, input_filename, output_filename_base):
        self.reduce_id_list = {}
        self.chunkState = {}
        self.chunkWorker = {}
        self.ready_chunks_mappers = {}
        for w in self.reduceState:
            if self.reduceState[w] == 'REDUCEDONE':
                self.reduceState[w] = 'READY'
        if not os.path.exists(os.getcwd()+'/'+self.data_dir+'/' + output_filename_base):
            os.makedirs(os.getcwd()+'/'+self.data_dir+'/' + output_filename_base,0777)
        chunks = self.split_file(input_filename, int(split_size))
        #Align map tasks to workers
        print 'MAP phase'
        print chunks
        l = len(chunks)
        for i in range(0,int(num_reducers)):
            self.reduce_id_list[i+1] = None

        for x in range(0,l):
            self.chunkState[chunks[x]] = 'CHUNK_NOTFINISH'
        while True:
            alldone = True
            finished_reducer_id_list = []
            for id in self.reduce_id_list:
                if self.reduce_id_list[id] is None:
                    continue
                elif self.reduceState[self.reduce_id_list[id]] == 'REDUCEDONE':
                    finished_reducer_id_list.append(id)
            #start mappers
            for i in range(0,l):
                if self.chunkState[chunks[i]] != 'CHUNK_FINISH' and self.chunkState[chunks[i]] != 'CHUNK_MAPPING' and self.chunkState[chunks[i]] != 'CHUNK_WAIT_REDUCEDONE':
                    w = self.select_a_mapper()
                    self.chunkWorker[chunks[i]] = w
                    if w != None:
                        self.mapState[w] = 'MAPSTART'
                        gevent.spawn(self.workers[w].do_map, job_name, input_filename, chunks[i], int(num_reducers), finished_reducer_id_list)

            #start reducers when map done
            #temp = int(num_reducers) - curr_num_reducers
            for i in range(0,int(num_reducers)):
                reduce_id = i+1
                if self.reduce_id_list[reduce_id] is None:
                    w = self.select_a_reducer()
                    self.reduce_id_list[reduce_id] = w
                    if w != None:
                        self.reduceState[w] = 'REDUCESTART'
                        gevent.spawn(self.workers[w].do_reduce, job_name, reduce_id, l)

            for i in range(0,l):
                w = self.chunkWorker[chunks[i]]
                if w is None:
                    continue
                if self.mapState[w] == 'LOSS':
                    self.chunkState[chunks[i]] = 'CHUNK_FAIL'
                    if self.ready_chunks_mappers.has_key(chunks[i]):
                        self.ready_chunks_mappers.pop(chunks[i])
                elif self.mapState[w] == 'MAPRESULTCOLLECT':
                    self.ready_chunks_mappers[chunks[i]] = w
                elif self.mapState[w] == 'MAPDONE':
                    self.chunkState[chunks[i]] = 'CHUNK_WAIT_REDUCEDONE'
                    if self.ready_chunks_mappers.has_key(chunks[i]):
                        self.ready_chunks_mappers.pop(chunks[i])
                    self.mapState[w] = 'READY'

                if self.reduceState[w] == 'LOSS':
                    for e in self.reduce_id_list:
                        if self.reduce_id_list[e] == w:
                            self.reduce_id_list[e] = None
                    for c in self.chunkState:
                        if self.chunkState[c] == 'CHUNK_WAIT_REDUCEDONE':
                            self.chunkState[c] = 'CHUNK_FAIL'
                elif self.reduceState[w] == 'REDUCESTART':
                    #send map list to reducer
                    print 'send mapperlist to reducer'
                    gevent.spawn(self.send_mapper_list, w)
                elif self.reduceState[w] == 'REDUCERESULTCOLLECT':
                    for id in self.reduce_id_list:
                        if self.reduce_id_list[id] == w:
                            gevent.spawn(self.write_reduce_result_list_to_file, w, input_filename, output_filename_base, id)
                elif self.reduceState[w] == 'REDUCEDONE':
                    pass
                    #self.chunkState[chunks[i]] = 'CHUNK_FINISH'


            print self.chunkState
            print 'ready_chunk_mappers:'
            print self.ready_chunks_mappers
            print 'reduceState:'
            print self.reduceState
            gevent.sleep(1)

            #judge if task is over
            num_finished_reducer = 0
            for w in self.reduceState:
                if self.reduceState[w] == 'REDUCEDONE':
                    num_finished_reducer += 1
            print num_finished_reducer, num_reducers
            if num_finished_reducer >= int(num_reducers):
                alldone = True
            else:
                alldone = False
            print alldone
            if alldone:
                break

        #while(i<l):
        #    for w in self.workers:
        #        if self.workers[w][0] == 'READY':
        #            print 'let him do map'
        #            #self.chunkState[chunks[i]] = ('NOTFINISH',w)
        #            gevent.spawn(self.workers[w][1].do_map, job_name, input_filename, chunks[i])
        #            i = i+1

        #Wait until all map tasks done
        print 'tasks done'

    def send_mapper_list(self, w):
        try:
            print self.ready_chunks_mappers
            list = []
            for e in self.ready_chunks_mappers:
                list.append((e, self.ready_chunks_mappers[e]))
            self.workers[w].set_mapper_list(list)
        except Exception:
            print 'Time out'

    def get_valid_mappers(self):
        list = []
        for w in self.mapState:
            if self.mapState[w] != 'LOSS':
                list.append(w)
        return list

    def select_a_reducer(self):
        selected_worker = None
        for w in self.workers:
            if self.reduceState[w] == 'READY':
                selected_worker = w
                break
        return selected_worker

    def select_a_mapper(self):
        selected_worker = None
        for w in self.workers:
            if self.mapState[w] == 'READY':# or self.mapState[w] == 'MAPDONE':
                selected_worker = w
                break
        return selected_worker

    def split_file(self, filename, split_size):
        #fileSize = os.path.getsize(filename)
        #print(fileSize)
        chunks = []
        with open(os.getcwd()+'/'+self.data_dir+'/'+filename) as inputfile:
            current_size = 0
            offset = 0
            #outputfile = open('sub_inputfile_' + str(index) + '.txt', 'w')
            for line in inputfile:
                #print line
                current_size += len(line)
                offset += len(line)
                if current_size >= split_size:
                    current_size -= len(line)
                    offset -= len(line)
                    words = line.split(' ')
                    #print words
                    for w in words:
                        current_size += len(w)
                        offset += len(w)
                        if not w.endswith('\n'):
                            current_size += 1
                            offset += 1
                        print current_size
                        if current_size >= split_size:
                            done = True
                            chunks.append((current_size, offset-current_size))
                            current_size = 0


            #the last chunk
            if current_size > 0:
                chunks.append((current_size, offset-current_size))
        return chunks


if __name__ == '__main__':
    port = sys.argv[1]
    data_dir = sys.argv[2]
    if not os.path.exists(os.getcwd()+'/'+data_dir):
        print 'no such directory'
    else:
        master_addr = 'tcp://0.0.0.0:' + str(port)
        s = zerorpc.Server(Master(data_dir))
        s.bind(master_addr)
        s.run()
    #m = Master('/')
    #print m.split_file('inputfile3.txt', 16)
    #print socket.gethostbyname(socket.gethostname())
