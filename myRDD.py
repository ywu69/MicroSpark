__author__ = 'pengzhan'
import itertools
import operator
import zerorpc
import gevent


class RDD(object):
    def __init__(self, prev=None, func=None):
        self.pipeID = 0 #record running steps.
        self.cacheInThisStep = False
        if not isinstance(prev, RDD):
            self.prev = None
            self.isFirst = True
            if func is None:
                def pipeline_func(iterator):
                    print self.pipeID
                    ret = map(lambda x:x,iterator)
                    if self.cacheInThisStep:
                        self.datalist = ret
                    return ret
                self.func = pipeline_func
            else:
                self.func = func
            self.numPartition = 1
            self.workerlist = {}
            self.workerIndex = 1
        else:
            self.prev = prev
            prev_func = prev.func
            def pipeline_func(iterator):
                if prev.isCached:
                    ret = func(prev.getDataList())
                else:
                    ret = func(prev_func(iterator))
                self.pipeID = prev.pipeID + 1
                print self.pipeID, "running "+str(func)
                if self.cacheInThisStep and self.isCached == False:
                    print "manual cache"
                    self.datalist = ret
                    self.isCached = True
                return ret
            self.func = pipeline_func
            self.numPartition = prev.numPartition
            self.workerlist = prev.workerlist #a map of ( (ip,port) -> index)
            self.workerIndex = prev.workerIndex
        self.datalist = []
        self.isCached = False
        self.runningID = 0
        self.input_filename = ""

        self.hashBucket = []

    def getDataList(self):
        return self.datalist

    def setDataList(self, dlist):
        self.datalist = dlist

    def getPartitionNum(self):
        return self.numPartition

    def calculate(self):
        return self.func([])

    def collect(self):
        return self.calculate()
        pass
        #1. send serialized RDD to each worker

        #2. worker do the calculation

        #3. collect all results

    def collectLocal(self):
        if self.func is None:
            return self.getDataList()
        return self.func(self.getDataList())
    # map(f :T->U) : RDD[T] -> RDD[U]

    def map(self, f):
        def func(iterator):
            return map(f, iterator)
        return RDD(self, func)

    # filter(f:T->Bool) : RDD[T] -> RDD[T]
    def filter(self,f):
        def func(iterator):
            return filter(f, iterator)
        return RDD(self,func)

    # flatMap( f : T -> Seq[U]) : RDD[T] -> RDD[U]
    def flatMap(self, f):
        def func(iterator):
            return list(itertools.chain.from_iterable(map(f, iterator)))
        return RDD(self,func)


    ##########################################

    def __mergeKeys(self,dic,l):
        for i in l:
            dic[i] = 1
        return dic

    def __getAllKeys(self):
        allKeys = {}
        for w in self.workerlist:
            if self.workerlist[w] == self.workerIndex:
                dic = {}
                for i in self.datalist:
                    dic[i[0]] = 1
                remoteKeys = dic.keys()
            else:
                c = zerorpc.Client(timeout=50)
                print "connect to:" + str(w)
                c.connect("tcp://"+w)

                remoteKeys = c.getKeys(self.pipeID)######################### CALL WORKER.getKeys(), return [a,b..]
                c.close()

            allKeys = self.__mergeKeys(allKeys,remoteKeys)
        return sorted(allKeys.keys())

    def __getAllKeyValues(self,keys):
        keyValues = []
        done = False
        while done is False:
            for w in self.workerlist:
                if self.workerlist[w] == self.workerIndex:
                    dict = {}

                    for key in keys:
                        dict[key] = 1

                    for i in self.datalist:
                        if dict.get(i[0], None) is not None:
                            keyValues.append(i)
                    # for i in self.datalist:
                    #     if i[0] in keys:
                    #         keyValues.append(i)
                    # print str(dict)
                else:
                    # try:
                    c = zerorpc.Client(timeout=50)
                    c.connect("tcp://"+w)
                    tp = c.getKeyValues(keys,self.pipeID)############# CALL WORKER.getKeyValues(), return [(a,1),(b,1)..].
                    # except Exception:
                    #     gevent.sleep(1)
                    #     keyValues = []
                    #     break
                    # each tuple element (x,y) will be convert to [x,y] by zerorpc, so convert [[a,1],[b,1]..] back to
                    # [(a,1),(b,1)..] in the following step
                    for i in tp:
                        keyValues.append((i[0],i[1]))
                    c.close()
                    done = True

            if done is False:
                gevent.sleep(1)
        return keyValues

    #def __getAllKeysTEST(self):
    #    return ['a','b','c']
    #def __getAllKeyValuesTEST(self,keys):
    #    ret = []
    #    l = [('a',1),('b',1),('c',1),('c',1)]
    #    for i in l:
    #        if i[0] in keys:
    #            ret.append(i)
    #    return ret



    def __get_hash(self):
        # init bucket
        bucket = []
        for i in range(0, len(self.workerlist)):
            bucket.append([])

        for i in self.datalist:
            bucket[hash(i[0]) % len(self.workerlist)].append(i[0])
        return bucket

    def __groupByKey_hash(self,iterator):
        #print "current pipe=",self.pipeID
        #print "prev res = ",iterator
        self.datalist = iterator
        self.isCached = True
        keyValues = []

        self.hashBucket = self.__get_hash()

        for i in range(0, len(self.hashBucket)):
            print str(len(self.hashBucket[i])),
        print

        for w in self.workerlist:
            if str(self.workerlist[w]) == str(self.workerIndex):
                keys = self.hashBucket[self.workerIndex-1]

                # optimize Time: O(n)  Extra Space
                dict = {}

                for key in keys:
                    dict[key] =[]

                for i in self.datalist:
                    if dict.get(i[0], None) is not None:
                        keyValues.append((i[0], i[1]))
            else:

                c = zerorpc.Client(timeout=50)
                c.connect("tcp://"+w)
                tp = c.getKeyValuesByHash(self.pipeID, self.workerIndex)############# CALL WORKER.getKeyValues(), return [(a,1),(b,1)..].
                # each tuple element (x,y) will be convert to [x,y] by zerorpc, so convert [[a,1],[b,1]..] back to
                # [(a,1),(b,1)..] in the following step
                for i in tp:
                    keyValues.append((i[0],i[1]))
                c.close()
                # print "current KeyValues:" + str(keyValues)

        dic = {}
        for x in keyValues:
            if isinstance(x,tuple):
                dic[x[0]] = []
        for x in keyValues:
            if not isinstance(x[1],list):
                x = (x[0],[x[1]])
            dic[x[0]].extend(x[1])
        return map(lambda x:(x,dic[x]),dic)

    def __groupByKey(self,iterator):
        self.datalist = iterator
        self.isCached = True
        print "auto cache"
        allKeys = self.__getAllKeys()
        keysIneed = []
        size = len(allKeys)/self.getPartitionNum()
        indexCnt = 1
        cnt = 0
        for k in allKeys:
            cnt += 1
            if indexCnt == self.workerIndex:
                keysIneed.append(k)
            if cnt >= size and indexCnt<self.getPartitionNum():
                cnt = 0
                indexCnt += 1
        keyValuesIneed = self.__getAllKeyValues(keysIneed)
        dic = {}
        for x in keyValuesIneed:
            if isinstance(x,tuple):
                dic[x[0]] = []
        for x in keyValuesIneed:
            if not isinstance(x[1],list):
                x = (x[0],[x[1]])
            dic[x[0]].extend(x[1])
        return map(lambda x:(x,dic[x]),dic)

    def groupByKey(self):
        #>>> x = sc.parallelize([("a", 1), ("b", 1), ("a", 1)])
        #>>> map((lambda (x,y): (x, list(y))), sorted(x.groupByKey().collect()))
        #[('a', [1, 1]), ('b', [1])]
        def func(iterator):
            return self.__groupByKey(iterator)
        return RDD(self,func)

    def reduceByKey(self,f):
        def func(iterator):
            ret = []
            tplist = self.__groupByKey(iterator)
            for i in tplist:
                ret.append((i[0],reduce(f,i[1])))
            return ret
        return RDD(self,func)

    def groupByKey_Hash(self):
        #>>> x = sc.parallelize([("a", 1), ("b", 1), ("a", 1)])
        #>>> map((lambda (x,y): (x, list(y))), sorted(x.groupByKey().collect()))
        #[('a', [1, 1]), ('b', [1])]
        def func(iterator):
            return self.__groupByKey_hash(iterator)
        return RDD(self,func)

    def reduceByKey_Hash(self,f):
        def func(iterator):
            ret = []
            tplist = self.__groupByKey_hash(iterator)
            for i in tplist:
                ret.append((i[0],reduce(f,i[1])))
            return ret
        return RDD(self,func)
    #############################

    # join() : (RDD[(K, V)], RDD[(K, W)]) -> RDD[(K, (V, W))]
    # cogroup() : (RDD[(K, V)], RDD[(K, W)]) -> RDD[(K, (Seq[V], Seq[W]))]
    # crossProduct() : (RDD[T], RDD[U]) -> RDD[(T, U)]
    # mapValues( f : V -> W) : RDD[(K, V)] -> RDD[(K, W)] (Preserves partitioning)
    # sort(c : Comparator[K]) : RDD[(K, V)] -> RDD[(K, V)]
    # partitionBy( p : Partitioner[K]) : RDD[(K, V)] -> RDD[(K, V)]


    def TextFile(self, filename):
        def func(yyy):
            ret = []
            f = open(filename)
            lines = f.readlines()
            size = len(lines) / self.numPartition
            cnt = 0
            index = 1
            for l in lines:
                if cnt >= size*(self.workerIndex-1) and cnt < size * self.workerIndex:
                    ret.append(l)
                elif cnt>=size*self.workerIndex and self.workerIndex == self.numPartition:
                    ret.append(l)
                cnt += 1
            f.close()
            return ret
        return RDD(self,func)

    def cache(self):
        self.cacheInThisStep = True
        return self

    """
    this is used by master
    It will re-init from backward:
        1.numPartition
        2.workerlist ([ip:port]->workerIndex)
    """
    def set_params_recv(self, workerlist):
        print "start set params"
        current = self
        num_partition = len(workerlist.keys())
        # assgin to last one
        current.numPartition = num_partition
        current.workerlist = workerlist

        # assign recursively
        while True:
            try:
                current = current.prev
                if current is not None:
                    current.numPartition = num_partition
                    current.workerlist = workerlist
            except AttributeError:
                break

    """
    this is used by worker
    It will re-init from backward:
        workerIndex: base on given [ip:port]
    """
    def set_worker_index_recv(self, ip, port):
        workerIndex = self.workerlist[str(ip) + ":" + str(port)]
        current = self
        while True:
            try:
                current = current.prev
                if current is not None:
                    current.workerIndex = workerIndex
            except AttributeError:
                break

    def get_ancester(self):

        current = self
        ancester_holder = current
        while True:
            try:
                current = current.prev
                if current is not None:
                    ancester_holder = current
            except AttributeError:
                break
        return ancester_holder

    def set_input_filename(self, input_filename):
        self.input_filename = input_filename

    def get_input_filename(self):
        print "get_input:" + str(self.input_filename)
        return self.input_filename


def test():
    #WordCount
    pass

if __name__ == '__main__':
    rdd = RDD()
    rdd.workerIndex = 2
    rdd.numPartition = 2
    rdd = rdd.TextFile('myfile')
    rdd = rdd.groupByKey()
    print rdd.collect()
    #print p.getPartitionByCondition(lambda x:x>=2, [1,2,3])
    #rdd = RDD(None,None,False,p)
    #rdd = rdd.map(lambda x:(x,1))#.groupByKeyLocal()
    #rdd = rdd.reduceByKeyLocal(operator.add)
    #print "datalist =", rdd.getDataList()
    #print "result =", rdd.collectLocal()

