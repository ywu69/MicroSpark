__author__ = 'pengzhan'
import itertools
import operator
import zerorpc
import gevent
import params
import StringIO
import cloudpickle
import copy
import sys


class RDD(object):
    def __init__(self, prev=None, func=None, master_address=None):
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
            self.master_address = master_address
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
            self.master_address = prev.master_address
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

    def collect_local(self):
        return self.calculate()

    def collect(self):
        #1. send serialized RDD to each worker
        #2. worker do the calculation
        #3. collect all results
        output = StringIO.StringIO()
        pickler = cloudpickle.CloudPickler(output)
        pickler.dump(self)#rdd
        pickle_object = output.getvalue()

        # if self.if_context is params.NO_CONTEXT:
        #     master_addr = self.master_address
        # elif self.if_context is params.USE_CONTEXT:
        #     master_addr = Context().getMasterAddress()
        # else:
        #     return "Unknown Type:" + str(self.if_context)
        #
        master_addr = self.master_address

        c = zerorpc.Client(timeout=params.GENERAL_TIMEOUT)
        c.connect("tcp://"+master_addr)
        c.set_job(pickle_object)

        worker_ips = c.result_is_ready()

        #print "####worker_ips for collect: " + str(worker_ips)

        final_results = []

        if worker_ips is None:
            print "#############################################"
            print "Insufficient worker(s) to finish partitions "
            print "#############################################"
            return None
        else:
            for w in worker_ips:
                c = zerorpc.Client(timeout=params.GENERAL_TIMEOUT)
                c.connect("tcp://"+w)

                result = c.getResults()
                if isinstance(result, int):
                    if isinstance(final_results, list):
                        final_results = 0
                    final_results += int(result)
                else:
                    final_results += result
            return final_results

    # not used
    def collectLocal(self):
        if self.func is None:
            return self.getDataList()
        return self.func(self.getDataList())
    # map(f :T->U) : RDD[T] -> RDD[U]

    def map(self, f):
        def func(iterator):
            return map(f, iterator)
        return RDD(self, func)

    # help me check on this
    def mapValues(self, f):
        def func(iterator):
            map_values_fn = lambda kv: (kv[0], f(kv[1]))
            return map(map_values_fn, iterator)
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

    # help me check on this
    def count(self):
        def func(iterator):
            return len(iterator)
        return RDD(self,func)

    def distinct(self):
        pass
        #
        # return
        #
        #
        # def func(iterator):
        #     return self.map(lambda x: (x, None)).\
        #         reduceByKey(lambda x, _: x).\
        #         map(lambda x: x[0])
        # return RDD(self,func)



    ##########################################

    def __mergeKeys(self,dic,l):
        for i in l:
            dic[i] = 1
        return dic

    def __getAllKeys(self):
        allKeys = {}

        done = False
        while done is False:
            for w in self.workerlist:
                if self.workerlist[w] == self.workerIndex:
                    dic = {}
                    for i in self.datalist:
                        dic[i[0]] = 1
                    remoteKeys = dic.keys()
                else:
                    try:
                        c = zerorpc.Client(timeout=params.GENERAL_TIMEOUT)
                        print "connect to:" + str(w)
                        c.connect("tcp://"+w)

                        remoteKeys = c.getKeys(self.pipeID)######################### CALL WORKER.getKeys(), return [a,b..]
                        c.close()
                    except Exception:
                        #allKeys = []
                        break
                        gevent.sleep(params.SLEEP_INTERVAL_GENERAL)
            done = True
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
                    try:
                        c = zerorpc.Client(timeout=params.GENERAL_TIMEOUT)
                        c.connect("tcp://"+w)
                        tp = c.getKeyValues(keys,self.pipeID)############# CALL WORKER.getKeyValues(), return [(a,1),(b,1)..].
                    except Exception:
                        gevent.sleep(params.SLEEP_INTERVAL_GENERAL)
                        keyValues = []
                        break
                    # each tuple element (x,y) will be convert to [x,y] by zerorpc, so convert [[a,1],[b,1]..] back to
                    # [(a,1),(b,1)..] in the following step
                    for i in tp:
                        keyValues.append((i[0],i[1]))
                    c.close()
            done = True

            if done is False:
                gevent.sleep(params.SLEEP_INTERVAL_GENERAL)
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

                c = zerorpc.Client(timeout=params.GENERAL_TIMEOUT)
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

    def join(self,rdd):

        #judge if same workerindex#
        #End Judge#
        def func(iterator):
            rdd_copy = copy.deepcopy(rdd)
            dic = {}
            for i in iterator:
                dic[i[0]] = None
            for i in iterator:
                if dic[i[0]] is None:
                    dic[i[0]] = [i[1]]
                else:
                    dic[i[0]] = dic[i[0]].append(i[1])
            lstofrdd = rdd_copy.calculate()
            print "dic = ",dic
            print "rdd = ",lstofrdd
            for i in lstofrdd:
                dic[i[0]].extend([i[1]])

            ret = []
            for i in dic:
                print i,dic[i]
                ret.append((i,dic[i]))
            print "ret = ",ret
            return ret
        return RDD(self,func)

    def set_input_filename(self, input_filename):
        self.input_filename = input_filename

    def get_input_filename(self):
        print "get_input:" + str(self.input_filename)
        return self.input_filename

    def set_master_address(self, master_address, type):
        self.master_address = master_address
        self.if_context = type
    def get_master_address(self):
        self.master_address

# import re
#
# def computeContribs(urls, rank):
#     """Calculates URL contributions to the rank of other URLs."""
#     num_urls = len(urls)
#     for url in urls:
#         yield (url, rank / num_urls)
#
# def parseNeighbors(urls):
#     """Parses a urls pair string into urls pair."""
#     parts = re.split(r'\s+', urls)
#     l =len(parts)
#     ls = []
#     for i in range(l):
#         if i == 0 or parts[i].strip() == '':
#             continue
#         else:
#             ls.append(parts[i])
#     return parts[0], ls

if __name__ == '__main__':
    rdd = RDD()
    rdd.workerlist ={"127.0.0.1:10000":1, "127.0.0.1:10001":2}
    rdd.workerIndex = int(sys.argv[1])
    links = rdd.TextFile("pagerank").map(lambda urls: parseNeighbors(urls)).cache()
    ranks = links.map(lambda url_neighbors: (url_neighbors[0], 1.0))

    for iteration in range(1):
        # Calculates URL contributions to the rank of other URLs.
        contribs = links.join(ranks).flatMap(
            lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
        print contribs.calculate()
        # Re-calculates URL ranks based on neighbor contributions.
        ranks = contribs.reduceByKey(operator.add).mapValues(lambda rank: rank * 0.85 + 0.15)

    # Collects all URL ranks and dump them to console.
    res = ranks.calculate()
    for (link, rank) in res:
        print("%s has rank: %s." % (link, rank/len(res)))