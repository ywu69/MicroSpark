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
                print '(%s,%s,%s)' % (w[0], w[1], self.workerState[w]),
            print
            gevent.sleep(1)

    def ping_worker(self,w):
        while True:
            if self.workerState[w] != "LOSS":
                try:
                    self.workers[w].ping()
                except Exception:
                    self.workerState[w] = "LOSS"
                    print 'lost connection'
                    break
            else:
                break
            gevent.sleep(1)

    def register_async(self, ip, port):
        print '[Master:%s] ' % self.state,
        print 'Registered worker (%s,%s)' % (ip, port)
        c = zerorpc.Client()
        c.connect("tcp://" + ip + ':' + port)
        self.workers[(ip,port)] = c
        self.workerState[(ip,port)] = 'READY'
        gevent.spawn(self.ping_worker,(ip,port))

        #####---ONLY FOR TESTING---########
        if len(self.workers) == 2:
            for w in self.workers:
                self.workers[w].cal()


    def register(self, ip, port):
        gevent.spawn(self.register_async, ip, port)

    def giveRDDWorkerInfo(self,rdd, worker_map, workerIndex):
        rdd.workerlist.clear()
        for e in worker_map:
            rdd.workerlist[e] = worker_map[e]
        while rdd != None:
            rdd.workerIndex = workerIndex
            rdd = rdd.prev


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

        worker_map = self.create_RDD_worker_map()


        #print f.collect()

        output = StringIO.StringIO()
        pickler = cloudpickle.CloudPickler(output)
        pickler.dump(intermediateRDD)
        pickle_object = output.getvalue()

        for w in self.workers:
            self.assign_rdd_to_worker(w, pickle_object)

        print "in setjob_async"
        pass

    def assign_rdd_to_worker(self, w, pickle_object):
        gevent.spawn(self.assign_rdd_to_worker_async, w, pickle_object)

    def assign_rdd_to_worker_async(self, w, pickle_object):
        c = zerorpc.Client()
        print str(w[0]) + ":" + str(w[1])
        c.connect("tcp://" + w[0] + ":" + w[1])
        c.setRDD(pickle_object)



    def create_RDD_worker_map(self):
        partition_map = {}
        i = 0
        for w in self.workers:
            partition_map[w[0] + ":" + w[1]] = i
            i += 1
        return partition_map

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
