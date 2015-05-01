__author__ = 'pengzhan'
import zerorpc
import sys
import socket
import gevent
import hamming
import mapreduce

class HammingEncodeMap(mapreduce.Map):
    def map(self, k, v):
        hb =  hamming.HammingBinary()
        for w in v:
            self.table[k] = [hb.encode(w)]
            k += 1

class HammingEncodeReduce(mapreduce.Reduce):
    def reduce(self, k, vlist):
        self.result_list[k] = vlist[0]

class EncodeReduce(mapreduce.Reduce):

    def reduce(self, k, vlist):
        pass

class WordCountMap(mapreduce.Map):

    def map(self, k, v):
        words = v.split()
        for w in words:
            if w in self.table:
                self.table[w] = [str(int(self.table[w][0]) + 1)]
            else:
                self.table[w] = ['1']
            #self.emit(w, '1')

class WordCountReduce(mapreduce.Reduce):

    def reduce(self, k, vlist):
        #print self.result_list
        count = 0
        for v in vlist:
            count = count + int(v)
        val = 0
        if self.result_list.has_key(k):
            val = self.result_list[k]
        val += count
        self.result_list[k] = val
        #self.emit(k + ':' + str(count))

class Worker(object):
    def __init__(self, master_addr, worker_ip, worker_port, test_flag):
        self.master_addr = master_addr
        self.worker_port = worker_port
        self.worker_ip = worker_ip
        self.c = zerorpc.Client()
        self.c.connect("tcp://"+master_addr)
        self.c.register(worker_ip, worker_port)

        #Attributes of mapper
        self.map_table = {}
        self.num_reducers = 0
        self.reduce_state = []
        gevent.spawn(self.controller)

        #Attributes of reducer
        self.mappers_list = {}
        self.map_result_collect_state = {}
        self.reduce_id = 0
        self.result_list = {}
        self.result_sent_to_master = False

        self.test_flag = test_flag

    def controller(self):
        while True:
            print('[Worker]')
            gevent.sleep(1)

    def ping(self):
        pass
        #print('[Worker] Ping from Master')


    def notice_received(self, reduce_id):
        gevent.spawn(self.notice_received_async, reduce_id)
    def notice_received_async(self,reduce_id):
        #print self.reduce_state
        print "reducer"+str(reduce_id)+ ' collected intermidiate data'
        self.reduce_state[reduce_id-1] = True

    def do_map(self, job_name, input_filename, chunk, num_reducers, finished_reducer_id_list):
        gevent.spawn(self.do_map_async, job_name, input_filename, chunk, num_reducers, finished_reducer_id_list)

    def do_map_async(self, job_name, input_filename, chunk, num_reducers, finished_reducer_id_list):
        self.reduce_state = []
        print finished_reducer_id_list
        for i in range(0,num_reducers):
            if (i+1) in finished_reducer_id_list:
                self.reduce_state.append(True)
            else:
                self.reduce_state.append(False)

        size = chunk[0]
        offset = chunk[1]
        print 'Doing MAP. size = '+ str(size)+', ' + 'offset = '+str(offset)
        self.c.set_chunk_state(size, offset, 'CHUNK_MAPPING')
        self.c.set_worker_map_state(self.worker_ip, self.worker_port, 'MAPPING')
        #print 'size = '+ str(size)
        #print 'offset = ' + str(offset)
        #DO MAP TASK
        job_name_map = job_name+'MAP'
        if job_name != 'wordcount':
            mapper = HammingEncodeMap()
        else:
            mapper = WordCountMap()
        # Map phase
        with open(input_filename) as inputfile:
            curr_off = -1
            firstline_done = False
            for line in inputfile:
                curr_off += len(line)
                if not firstline_done and curr_off >= offset:
                    curr_off -= len(line)
                    words = line.split(' ')
                    newline = ''
                    findoff = False
                    #print words
                    for w in words:
                        curr_off += len(w)
                        if not w.endswith('\n'):
                            curr_off += 1
                        #print curr_off
                        if not findoff and curr_off >= offset:
                            findoff = True
                        if findoff and curr_off <= offset+size:
                            newline += w
                            if not w.endswith('\n'):
                                newline += ' '
                    #print newline
                    mapper.map(curr_off-len(newline), newline)
                    firstline_done = True

                elif firstline_done and curr_off < offset+size:
                    mapper.map(curr_off - len(line), line)
                elif curr_off >= offset+size:
                    curr_off -= len(line)
                    words = line.split(' ')
                    newline = ''
                    for w in words:
                        curr_off += len(w)
                        if not curr_off >= offset+size:
                            newline += w
                            if not w.endswith('\n'):
                                curr_off += 1
                                newline += ' '
                        if curr_off >= offset+size:
                            break
                    mapper.map(curr_off-len(newline), newline)

        # Sort intermediate keys
        self.map_table = mapper.get_table()
        self.num_reducers = num_reducers
        #print self.map_table

        print 'MAP: wait reducers to collect map data'
        self.c.set_worker_map_state(self.worker_ip, self.worker_port, 'MAPRESULTCOLLECT')
        alldone = False
        while not alldone:
            alldone = True
            for i in self.reduce_state:
                if i == False:
                    alldone = False
            gevent.sleep(1)
            print self.reduce_state
            continue

        self.c.set_worker_map_state(self.worker_ip, self.worker_port, 'MAPDONE')
        print 'MAP: Done'
        #send to reducer

    def do_reduce(self, job_name, reduce_id, num_chunk):
        gevent.spawn(self.do_reduce_async, job_name, reduce_id, num_chunk)

    def do_reduce_async(self, job_name, reduce_id, num_chunk):
        #wait until all map data collected
        print 'Doing REDUCE '+ job_name+' reduce id = '+str(reduce_id)
        self.map_result_collect_state = {}
        self.result_list = {}
        self.result_sent_to_master = False
        self.reduce_id = reduce_id
        if job_name != 'wordcount':
            reducer = HammingEncodeReduce()
        else:
            reducer = WordCountReduce()

        print 'REDUCE: get map datas'
        #print self.mappers_list
        while len(self.map_result_collect_state) < num_chunk:
            #print len(self.map_result_collect_state), num_chunk
            for e in self.mappers_list:
                if not self.map_result_collect_state.has_key(e):
                    gevent.spawn(self.reduce_single_map_result, reducer, e, reduce_id)
            gevent.sleep(1)
        if self.test_flag:
            print 'PRESS CTRL+C'
            gevent.sleep(3)

        self.c.set_worker_reduce_state(self.worker_ip, self.worker_port, 'REDUCERESULTCOLLECT')
        #alldone = False
        print 'REDUCE: Wait master to get'
        self.result_list = reducer.get_result_list()
        while not self.result_sent_to_master:
            gevent.sleep(1)
            continue
        print 'REDUCE: Done'
        self.c.set_worker_reduce_state(self.worker_ip, self.worker_port, 'REDUCEDONE')

    def master_notice_received(self):
        gevent.spawn(self.master_notice_received_async)

    def master_notice_received_async(self):
        self.result_sent_to_master = True

    def reduce_single_map_result(self,reducer, chunk, reduce_id):
        try:
            w = self.mappers_list[chunk]
            c = zerorpc.Client()
            c.connect("tcp://"+w[0]+':'+w[1])
            table = c.get_map_table_part(reduce_id)
            c.notice_received(reduce_id)
        #print table
            self.map_result_collect_state[chunk] = 'COLLECTED'
            for e in table:
                reducer.reduce(e[0], e[1])
        except Exception:
            print 'Time out, wait until mapperlist update'
        #keys = table.keys()
        #for k in keys:
        #    reducer.reduce(k, table[k])

    def set_mapper_list(self, mappers_list):
        gevent.spawn(self.set_mapper_list_async, mappers_list)

    def set_mapper_list_async(self,mappers_list):
        for e in mappers_list:
            key = (e[0][0], e[0][1])
            val = (e[1][0], e[1][1])
            self.mappers_list[key] = val
        #print self.mappers_list

    def get_map_table_part(self, reduce_id):
        table = []
        keys = self.map_table.keys()
        #print keys
        each_size = len(keys)/self.num_reducers
        #print 'eachsize = ' + str(each_size)
        begin = (reduce_id-1)*each_size

        if reduce_id == self.num_reducers:
            end = len(keys)
        else:
            end = reduce_id*each_size

        tp = 0
        #print begin, end
        for e in sorted(self.map_table):
            if tp>=begin and tp < end:
                table.append((e,self.map_table[e]))
            tp += 1
        #print table
        return table


    def get_result_list(self):
        list = []
        for e in sorted(self.result_list):
            list.append((e,self.result_list[e]))
        return list

if __name__ == '__main__':
    worker_ip = socket.gethostbyname(socket.gethostname())
    worker_port = sys.argv[1]
    master_addr = sys.argv[2];
    test_flag = False
    if len(sys.argv) > 3:
        test_flag = sys.argv[3]=="test"
    s = zerorpc.Server(Worker(master_addr,worker_ip,worker_port,test_flag))
    s.bind('tcp://' + worker_ip+":"+worker_port)
    s.run()

    #w = Worker('A','B',1000)
    #chunk = (11,36)
    #w.do_map('a', 'inputfile2.txt', chunk)
    #print socket.gethostbyname(socket.gethostname())