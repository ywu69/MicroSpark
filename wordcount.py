import sys

#import mapreduce_class as mapreduce
import mapreduce

class WordCountMap(mapreduce.Map):

    def map(self, k, v):
        words = v.split()
        for w in words:
            self.emit(w, '1')

class WordCountReduce(mapreduce.Reduce):

    def reduce(self, k, vlist):
        count = 0
        for v in vlist:
            count = count + int(v)
        self.emit(k + ':' + str(count))

if __name__ == '__main__':

    f = open(sys.argv[1])
    values = f.readlines()
    f.close()

    engine = mapreduce.Engine(values, WordCountMap, WordCountReduce)
    engine.execute()
    result_list = engine.get_result_list()
    
    for r in result_list:
        print r
