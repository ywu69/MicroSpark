__author__ = 'pengzhan'
import zerorpc
import sys
import socket
import gevent
import StringIO
import pickle

class Worker(object):
    def __init__(self, master_addr, worker_ip, worker_port):
        self.master_addr = master_addr
        self.worker_port = worker_port
        self.worker_ip = worker_ip
        self.c = zerorpc.Client()
        self.c.connect("tcp://"+master_addr)
        self.c.register(worker_ip, worker_port)
        self.RDD = ""

    def controller(self):
        while True:
            print('[Worker]')
            gevent.sleep(1)

    def ping(self):
        pass
        print('[Worker] Ping from Master')

    def setRDD(self, RDD):
        input = StringIO.StringIO(RDD)
        unpickler = pickle.Unpickler(input)
        self.RDD = unpickler.load()
        # set current partition
        self.RDD.get_ancester().set_current_partition(worker_ip, worker_port)
        print self.RDD.collect()

if __name__ == '__main__':
    worker_ip = socket.gethostbyname(socket.gethostname())
    worker_port = sys.argv[1]
    master_addr = 'localhost:4242'#sys.argv[2];
    s = zerorpc.Server(Worker(master_addr,worker_ip,worker_port))
    s.bind('tcp://' + worker_ip+":"+worker_port)
    s.run()

    #w = Worker('A','B',1000)
    #chunk = (11,36)
    #w.do_map('a', 'inputfile2.txt', chunk)
    #print socket.gethostbyname(socket.gethostname())