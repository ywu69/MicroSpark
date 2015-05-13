__author__ = 'pengzhan'
import zerorpc
import sys
import socket
import gevent
import operator
from myRDD import RDD
import StringIO
import pickle
import params

class Worker(object):
    def __init__(self, master_addr, worker_ip, worker_port, type):
        self.master_addr = master_addr
        self.worker_port = worker_port
        self.worker_ip = worker_ip
        self.RDD = RDD()
        self.c = zerorpc.Client(timeout=500)
        self.c.connect("tcp://"+master_addr)
        self.c.register(worker_ip, worker_port, type)
        self.results = ""

    def controller(self):
        while True:
            print('[Worker]')
            gevent.sleep(params.SLEEP_INTERVAL_GENERAL)


    def ping(self):
        print('[Worker] Ping from Master')
        pass

    def update_RDD_workerlist(self, workerlist):
        print "start update: " + str(workerlist)
        try:
            self.RDD.set_params_recv(workerlist)
        except Exception:
            print "########### fail to update workerlist, try again later"
        print "done"

    def getKeyValues(self, keys, pipeID):
        while pipeID > self.getCurrentPipeID():
            gevent.sleep(params.SLEEP_INTERVAL_GENERAL)
        rdd = self.getRDDByPipeID(pipeID)
        ret = []
        dict = {}

        for key in keys:
            dict[key] = 1

        for i in rdd.datalist:
            if dict.get(i[0], None) is not None:
                ret.append(i)
        # for i in rdd.datalist:
        #     if i[0] in keys:
        #         ret.append(i)
        return ret

    def __get_hash(self, pipeID):

        while pipeID > self.getCurrentPipeID():
            gevent.sleep(params.SLEEP_INTERVAL_GENERAL)
        rdd = self.getRDDByPipeID(pipeID)

        while len(rdd.hashBucket) == 0:
            gevent.sleep(params.SLEEP_INTERVAL_SHORT)
        return rdd.hashBucket

    def getKeyValuesByHash(self, pipeID, remote_worker_index):

        while pipeID > self.getCurrentPipeID():
            gevent.sleep(params.SLEEP_INTERVAL_GENERAL)
        rdd = self.getRDDByPipeID(pipeID)

        bucket = rdd.hashBucket
        keys = bucket[remote_worker_index-1]
        ret = []

        dict = {}

        for key in keys:
            dict[key] =[]

        for i in rdd.datalist:
            if dict.get(i[0], None) is not None:
                ret.append((i[0], i[1]))
        return ret

    def getKeys(self, pipeID):
        while pipeID > self.getCurrentPipeID():
            gevent.sleep(params.SLEEP_INTERVAL_GENERAL)

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
        gevent.spawn(self.setRDD_async, RDD)

    def setRDD_async(self, RDD):
        self.results = ""
        self.RDD = ""

        self.c.set_worker_state(self.worker_ip, self.worker_port, 'WORKING')

        input = StringIO.StringIO(RDD)
        unpickler = pickle.Unpickler(input)
        self.RDD = unpickler.load()
        # set current partition
        self.RDD.set_worker_index_recv(self.worker_ip, self.worker_port)
        # collect
        self.results = self.RDD.collect_local()
        # print "My results:" + str(self.results)
        self.c.set_worker_state(self.worker_ip, self.worker_port, 'FINISHED')

    def getResults(self):
        self.c.set_worker_state(self.worker_ip, self.worker_port, 'READY')
        return self.results

if __name__ == '__main__':
    worker_ip = socket.gethostbyname(socket.gethostname())
    worker_port = sys.argv[1]
    type = sys.argv[2]

    master_addr = sys.argv[3];
    w = Worker(master_addr,worker_ip,worker_port, type)

    s = zerorpc.Server(w)
    s.bind('tcp://' + worker_ip+":"+worker_port)
    s.run()


    #w = Worker('A','B',1000)
    #chunk = (11,36)
    #w.do_map('a', 'inputfile2.txt', chunk)
    #print socket.gethostbyname(socket.gethostname())