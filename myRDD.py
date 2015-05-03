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

    def __init__(self, prev=None, func=None, isCached=False, partition=None):
        if not isinstance(prev, RDD):
            self.isFirst = True
            if func is None:
                def pipeline_func(iterator):
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

    def getPartition(self):
        return self.partition

    def getDataList(self):
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
    def filter(self,f):
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
        def func(yyy):
            f = open(filename)
            iterator = f.readlines()

            f.close()
            return iterator
        return RDD(self,func)

    def cache(self):
        self.setDataList(self.collect())
        self.isCached = True
        return self


def test():
    pass

if __name__ == '__main__':
    test()
    #s = ""
    #print map(lambda x:x.split(" ") , ['1 2 4 5', '2 3 5 7'])
    p = Partition(1,['a','b','c','c'])
    rdd = RDD(None,None,False,p)
    rdd = rdd.map(lambda x:(x,1))#.groupByKey()
    rdd = rdd.reduceByKey(operator.add)
    print "datalist =", rdd.getDataList()
    print "result =", rdd.collect()


