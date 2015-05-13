__author__ = 'blu2'

import sys
import os
import zerorpc
from mr_master import Master
from mr_worker import Worker
import gevent
from myRDD import RDD


class Context(object):

    def __init__(self):
        self.master_ip = "localhost"
        self.master_port = "4242"
        self.data_dir = '/'#sys.argv[2]

        if not os.path.exists(os.getcwd()+'/'+self.data_dir):
            print 'no such directory'
        else:
            pass



    def init(self):
        print "create master"
        self.create_master()
        self.create_worker("0.0.0.0", "10000", "1")
        self.create_worker("0.0.0.0", "10001", "1")
        self.create_worker("0.0.0.0", "10002", "1")
        #self.create_worker("0.0.0.0", "10003", "1")
        #self.create_worker("0.0.0.0", "10004", "1")
        return RDD(None, None, self.getMasterAddress())

    def create_master(self):
        gevent.spawn(self.create_master_async)

    def create_master_async(self):
        master_addr = 'tcp://0.0.0.0:' + str(self.master_port)
        s = zerorpc.Server(Master(self.data_dir))
        s.bind(master_addr)
        s.run()


    def getMasterAddress(self):
        return self.master_ip + ":" + self.master_port


    def create_worker(self, worker_ip, worker_port, type):
        gevent.spawn(self.create_worker_async, worker_ip, worker_port, type)

    def create_worker_async(self, worker_ip, worker_port, type):
        master_addr = self.getMasterAddress()
        w = Worker(master_addr,worker_ip,worker_port, type)
        # w = Worker("localhost:4242", "localhost", "10000", "1")
        s = zerorpc.Server(w)
        s.bind('tcp://' + worker_ip+":"+worker_port)
        s.run()



