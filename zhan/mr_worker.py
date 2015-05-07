__author__ = 'pengzhan'
import zerorpc
import sys
import socket
import gevent
import operator
from myRDD import RDD

class Worker(object):
    def __init__(self, master_addr, worker_ip, worker_port):
        self.master_addr = master_addr
        self.worker_port = worker_port
        self.worker_ip = worker_ip
        self.rdd = RDD()
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
        ret = self.rdd.calculate()
        print "res = ",ret
        r = self.rdd
        while r != None:
            print "data cached on rdd pipe"+str(r.pipeID)+": ", r.datalist
            r = r.prev

    def getRDDByPipeID(self,pipeID):
        r = self.rdd
        while r!=None and r.pipeID == 0:
            r = r.prev
        if r == None or r.pipeID<pipeID: return None
        while r.pipeID>pipeID:
            r = r.prev
        return r


    def getCurrentPipeID(self):
        r = self.rdd
        while r!=None and r.pipeID == 0:
            r = r.prev
        if r == None: return 0
        else: return r.pipeID

if __name__ == '__main__':
    worker_ip = socket.gethostbyname(socket.gethostname())
    worker_port = sys.argv[1]
    worker_index = sys.argv[2]
    master_addr = '127.0.0.1:4242'#sys.argv[2];
    w = Worker(master_addr,worker_ip,worker_port)

    ##Initialize rdd (SHOULD BE DONE BY MASTER)
    w.rdd = RDD()
    w.rdd.isCached = True

    w.rdd.workerlist[worker_ip+':8000'] = 1
    w.rdd.workerlist[worker_ip+':8001'] = 2
    if worker_index == str(1):
        #w.rdd.datalist = ['a','xx','c']
        w.rdd.workerIndex = 1
    else:
        #w.rdd.datalist = ['a','b']
        w.rdd.workerIndex = 2

    w.rdd.numPartition = 2
    #w.rdd = w.rdd.map(lambda x:(x,1)).reduceByKey(operator.add)
    w.rdd = w.rdd.TextFile('myfile').flatMap(lambda x:x.split()).map(lambda x:(x,1)).reduceByKey(operator.add)
    ##################
    s = zerorpc.Server(w)
    s.bind('tcp://' + worker_ip+":"+worker_port)
    s.run()


    #w = Worker('A','B',1000)
    #chunk = (11,36)
    #w.do_map('a', 'inputfile2.txt', chunk)
    #print socket.gethostbyname(socket.gethostname())