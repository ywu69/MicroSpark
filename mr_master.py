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
