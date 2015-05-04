__author__ = 'pengzhan'
import sys

import zerorpc
import os
import gevent
import socket
from gevent import timeout
import StringIO
import pickle
import cloudpickle

class Master(object):

    def __init__(self, data_dir):
        gevent.spawn(self.controller)
        self.state = 'READY'
        self.data_dir = data_dir

        self.workers = {}
        self.workerState = {}

    def controller(self):
        while True:
            print '[Master:%s] ' % (self.state),
            for w in self.workers:
                gevent.spawn(self.ping_worker,w)
                print '(%s,%s,%s)' % (w[0], w[1], self.workerState[w]),
            print
            gevent.sleep(1)

    def ping_worker(self,w):
        if self.workerState[w] != "LOSS":
            try:
                self.workers[w].ping()
            except Exception:
                self.workerState[w] = "LOSS"
                print 'lost connection'

    def register_async(self, ip, port):
        print '[Master:%s] ' % self.state,
        print 'Registered worker (%s,%s)' % (ip, port)
        c = zerorpc.Client()
        c.connect("tcp://" + ip + ':' + port)
        self.workers[(ip,port)] = c
        self.workerState[(ip,port)] = 'READY'
        c.ping()

    def register(self, ip, port):
        gevent.spawn(self.register_async, ip, port)


    def set_job(self, pickle_object):
        gevent.spawn(self.setJob_async, pickle_object)

    def setJob_async(self, pickle_object):

        # unpickle
        input = StringIO.StringIO(pickle_object)
        unpickler = pickle.Unpickler(input)
        intermediateRDD = unpickler.load()

        input_filename = intermediateRDD.get_ancester().get_input_filename()

        # set partition as num of workers
        intermediateRDD.get_ancester().set_partition(len(self.workers))

        chunks = self.split_file(input_filename, len(self.workers))
        partition_map = self.create_RDD_partition_map(chunks)
        print partition_map
        intermediateRDD.get_ancester().set_partition_map(partition_map)

        #print f.collect()

        output = StringIO.StringIO()
        pickler = cloudpickle.CloudPickler(output)
        pickler.dump(intermediateRDD)
        pickle_object = output.getvalue()

        for w in self.workers:
            c = zerorpc.Client()
            print str(w[0]) + ":" + str(w[1])
            c.connect("tcp://" + w[0] + ":" + w[1])
            c.setRDD(pickle_object)

        print "in setjob_async"

        pass


    def create_RDD_partition_map(self, chunks):
        if len(chunks) != len(self.workers):
            print "ERROR"

        partition_map = {}
        i = 0
        for w in self.workers:
            partition_map[w[0] + ":" + w[1]] = chunks[i]
            i += 1
        return partition_map



    def split_file(self, filename, num_worker):
        fileSize = os.path.getsize(filename)
        chunks = []
        split_size = fileSize/num_worker
        with open(filename) as inputfile:
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

        # very special case
        if len(chunks) < num_worker:
            chunks.append((current_size, offset))

        return chunks



if __name__ == '__main__':
    port = 4242#sys.argv[1]
    data_dir = '/'#sys.argv[2]
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
