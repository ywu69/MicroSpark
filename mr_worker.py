__author__ = 'pengzhan'
import zerorpc
import sys
import socket
import gevent
import operator
from myRDD import RDD
import StringIO
import pickle

class Worker(object):
    def __init__(self, master_addr, worker_ip, worker_port):
        self.master_addr = master_addr
        self.worker_port = worker_port
        self.worker_ip = worker_ip
        self.RDD = RDD()
        self.c = zerorpc.Client()
        self.c.connect("tcp://"+master_addr)
        self.c.register(worker_ip, worker_port)

    def controller(self):
        while True:
            print('[Worker]')
            gevent.sleep(1)


    def ping(self):
        #print('[Worker] Ping from Master')
        pass

    def getKeyValues(self, keys, pipeID):
        while pipeID > self.getCurrentPipeID():
            gevent.sleep(1)
        rdd = self.getRDDByPipeID(pipeID)
        ret = []
        for i in rdd.datalist:
            if i[0] in keys:
                ret.append(i)
        return ret

    def getKeys(self, pipeID):
        while pipeID > self.getCurrentPipeID():
            gevent.sleep(1)

        rdd = self.getRDDByPipeID(pipeID)
        dic = {}
        for i in rdd.datalist:
            dic[i[0]] = 1
        return dic.keys()

    def cal(self):
        gevent.spawn(self.cal_async)

    def cal_async(self):
        ret = self.RDD.calculate()
        print "res = ",ret
        r = self.RDD
        while r != None:
            r = r.prev

    def getRDDByPipeID(self,pipeID):
        r = self.RDD
        while r!=None and r.pipeID == 0:
            r = r.prev
        if r == None or r.pipeID<pipeID: return None
        while r.pipeID>pipeID:
            r = r.prev
        return r


    def getCurrentPipeID(self):
        r = self.RDD
        while r!=None and r.pipeID == 0:
            r = r.prev
        if r == None: return 0
        else: return r.pipeID

    def setRDD(self, RDD):
        input = StringIO.StringIO(RDD)
        unpickler = pickle.Unpickler(input)
        self.RDD = unpickler.load()
        # set current partition
        self.RDD.set_worker_index_recv(worker_ip, worker_port)
        print self.RDD.collect()

if __name__ == '__main__':
    worker_ip = socket.gethostbyname(socket.gethostname())
    worker_port = sys.argv[1]

    master_addr = '127.0.0.1:4242'#sys.argv[2];
    w = Worker(master_addr,worker_ip,worker_port)

    s = zerorpc.Server(w)
    s.bind('tcp://' + worker_ip+":"+worker_port)
    s.run()


    #w = Worker('A','B',1000)
    #chunk = (11,36)
    #w.do_map('a', 'inputfile2.txt', chunk)
    #print socket.gethostbyname(socket.gethostname())