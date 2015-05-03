__author__ = 'pengzhan'
import itertools
class RDD(object):

    def __init__(self, prev=None, func=None, datalist = None, isCached=False):
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
                    return func(prev.datalist)
                return func(prev_func(iterator))
            self.func = pipeline_func
        self.isCached = isCached
        self.datalist = datalist

    def collect(self):
        if self.func is None:
            return self.datalist
        return self.func(self.datalist)

    # map(f :T->U) : RDD[T] -> RDD[U]
    def map(self, f):
        def func(iterator):
            return map(f, iterator)
        return RDD(self, func, self.datalist)

    # filter(f:T->Bool) : RDD[T] -> RDD[T]
    def filter(self,f):
        def func(iterator):
            return filter(f, iterator)
        return RDD(self, func, self.datalist)

    # flatMap( f : T -> Seq[U]) : RDD[T] -> RDD[U]
    def flatMap(self, f):
        def func(iterator):
            return list(itertools.chain.from_iterable(map(f, iterator)))
        return RDD(self, func, self.datalist)

    # sample(fraction : Float) : RDD[T] -> RDD[T] (Deterministic sampling)

    # groupByKey() : RDD[(K, V)] -> RDD[(K, Seq[V])]
    def groupByKey(self):
        def func(iterator):
            dic = {}
            for x in iterator:
                dic[x[0]] = []
            for x in iterator:
                if not isinstance(x[1],list):
                    x = (x[0],[x[1]])
                dic[x[0]].extend(x[1])
            return map(lambda x:[x,dic[x]],dic)
        return RDD(self, func, self.datalist)

    # reduceByKey( f : (V, V) -> V) : RDD[(K, V)] -> RDD[(K, V)]
    #>>> rdd = sc.parallelize([("a", 1), ("b", 1), ("a", 1)])
    #>>> sorted(rdd.reduceByKey(add).collect())
    #[('a', 2), ('b', 1)]
    def reduceByKey(self, f):
        def func(iterator):
            sel
        pass
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
        return RDD(self, func, self.datalist)

    def cache(self):
        self.datalist = self.collect()
        self.isCached = True
        return self

if __name__ == '__main__':
    #s = ""
    #print map(lambda x:x.split(" ") , ['1 2 4 5', '2 3 5 7'])
    rdd = RDD(None,None,[1,2,3,4]).map(lambda x:(x,[x*x])).groupByKey()
    print "datalist =", rdd.datalist;
    print "result =", rdd.collect();



