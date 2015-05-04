__author__ = 'pengzhan'
import itertools
import operator


class Partition(object):
    def __init__(self,index,datalist=None,blocklist={}):
        self.index = index
        self.num_of_part = len(blocklist)
        self.datalist = datalist
        self.blocklist = blocklist

class RDD(object):
    def __init__(self, prev=None, func=None, datalist = None, isCached=False, partition=None, current_partition=None):
        if not isinstance(prev, RDD):
            self.isFirst = True
            if func is None:
                def pipeline_func(iterator):
                    if iterator is None:
                        iterator = []
                    return map(lambda x:x,iterator)
                self.func = pipeline_func
            else:
                self.func = func
        else:
            self.prev = prev
            prev_func = prev.func
            def pipeline_func(iterator):
                if prev.isCached:
                    return func(prev.getDataList())
                return func(prev_func(iterator))
            self.func = pipeline_func

        #partition paras:
        self.isCached = isCached
        if partition is None and isinstance(prev, RDD):
            self.partition = prev.partition
        else:
            self.partition = partition

        self.current_partition = ()
        self.isCached = isCached
        self.datalist = datalist
        self.partition_map = {}
        self.input_filename = ""

    def getPartition(self):
        return self.partition

    def getDataList(self):
        if self.partition is None:
            return None

        return self.partition.datalist

    def setDataList(self, dlist):
        self.partition.datalist = dlist

    def collect(self):
        if self.func is None:
            return self.getDataList()
        return self.func(self.getDataList())

    # map(f :T->U) : RDD[T] -> RDD[U]
    def map(self, f):
        def func(iterator):
            return map(f, iterator)
        return RDD(self, func)

    # filter(f:T->Bool) : RDD[T] -> RDD[T]
    def filter(self, f):
        def func(iterator):
            return filter(f, iterator)
        return RDD(self,func)

    # flatMap( f : T -> Seq[U]) : RDD[T] -> RDD[U]
    def flatMap(self, f):
        def func(iterator):
            return list(itertools.chain.from_iterable(map(f, iterator)))
        return RDD(self,func)

    # sample(fraction : Float) : RDD[T] -> RDD[T] (Deterministic sampling)

    # groupByKey() : RDD[(K, V)] -> RDD[(K, Seq[V])]
    def groupByKey(self):
        def func(iterator):
            dic = self._groupByKey(iterator)
            return map(lambda x:(x,dic[x]),dic)
        return RDD(self,func)

    #return a dic
    def _groupByKey(self,iterator):
        dic = {}
        for x in iterator:
            if isinstance(x,tuple):
                dic[x[0]] = []
        for x in iterator:
            if not isinstance(x[1],list):
                x = (x[0],[x[1]])
            dic[x[0]].extend(x[1])
        return dic

    # reduceByKey( f : (V, V) -> V) : RDD[(K, V)] -> RDD[(K, V)]
    #>>> rdd = sc.parallelize([("a", 1), ("b", 1), ("a", 1)])
    #>>> sorted(rdd.reduceByKey(add).collect())
    #[('a', 2), ('b', 1)]
    def reduceByKey(self, f):
        def func(iterator):
            dic = self._groupByKey(iterator)
            return map(lambda x:(x,reduce(f,dic[x])),dic)
        return RDD(self,func)
    # union() : (RDD[T], RDD[T]) -> RDD[T]
    def union(self):
        pass
    # join() : (RDD[(K, V)], RDD[(K, W)]) -> RDD[(K, (V, W))]
    # cogroup() : (RDD[(K, V)], RDD[(K, W)]) -> RDD[(K, (Seq[V], Seq[W]))]
    # crossProduct() : (RDD[T], RDD[U]) -> RDD[(T, U)]
    # mapValues( f : V -> W) : RDD[(K, V)] -> RDD[(K, W)] (Preserves partitioning)
    # sort(c : Comparator[K]) : RDD[(K, V)] -> RDD[(K, V)]
    # partitionBy( p : Partitioner[K]) : RDD[(K, V)] -> RDD[(K, V)]


    def TextFile(self, filename):
        self.input_filename = filename

        def func(yyy):
            f = open(filename)
            print "map:" + str(self.partition_map)
            print "Tuple:" + str(self.get_current_partition())
            iterator = f.read()[self.current_partition[1]: self.current_partition[1] + self.current_partition[0]]
            f.close()
            return iterator
        return RDD(self,func)

    def get_ancester(self):

        current = self
        while True:
            try:
                current = current.prev
            except AttributeError:
                break
        return current


    def cache(self):
        self.setDataList(self.collect())
        self.isCached = True
        return self

    def test(self):
        pass

    def set_current_partition(self, ip, port):
        self.current_partition = self.partition_map[str(ip) + ":" + str(port)]
        print "current_partition: " + str(self.current_partition)

    def get_current_partition(self):
        return self.current_partition

    def set_partition(self, partition):
        self.partition = partition

    def get_partition(self):
        return self.partition

    def set_partition_map(self, partition_map):
        self.partition_map = partition_map

    def get_partition_map(self):
        return self.partition_map

    def set_input_filename(self, input_filename):
        self.input_filename = input_filename

    def get_input_filename(self):
        print "get_input:" + str(self.input_filename)
        return self.input_filename
if __name__ == '__main__':
    #test()
    #s = ""
    #print map(lambda x:x.split(" ") , ['1 2 4 5', '2 3 5 7'])
    p = Partition(1,['a','b','c','c'])
    rdd = RDD(None,None,False,p)
    rdd = rdd.map(lambda x:(x,1))#.groupByKey()
    rdd = rdd.reduceByKey(operator.add)
    print "datalist =", rdd.getDataList()
    print "result =", rdd.collect()

    R = RDD()

    rdd = R.TextFile("inputfile2.txt")

    print "try set parttion"
    rdd.set_partition(1)

    rdd.get_ancester().current_partition = (1,2)


    print rdd.get_input_filename()

    print "datalist =", rdd.datalist;
    print "result =", rdd.collect();
