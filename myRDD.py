__author__ = 'pengzhan'

class RDD(object):

    def __init__(self, prev, func, initList):
        self.numofP = 3
        self.block[0]=("127.0.0.1",8080)
        self.block[1]=("127.0.0.1",8081)
        self.block[2]=("127.0.0.1",8082)
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
                return func(prev_func(iterator))
            self.func = pipeline_func
        self.initList = initList

    def collect(self):
        if self.func is None:
            return self.initList
        return self.func(self.initList)

    def map(self, f):
        def func(iterator):
            return map(f, iterator)
        return RDD(self, func, self.initList)

    def TextFile(self, filename):
        def func(yyy):
            f = open(filename)
            iterator = f.readlines()

            f.close()
            return iterator
        return RDD(self, func, self.initList)

    def cache(self):
        l = self.collect()
        self.initList = l
        return self

if __name__ == '__main__':
    s = ""
    rdd = RDD(None,None,[1,2,3,4]).TextFile('myfile').map(lambda x:x+"oo").cache()
    print rdd.collect();
    print rdd.initList
