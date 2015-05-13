__author__ = 'blu2'

import zerorpc
import sys
from myRDD import *
import StringIO
import cloudpickle
from datetime import datetime
import params
from Context import Context


if __name__ == '__main__':
    C = Context()
    R = C.init()
    # rdd = R.TextFile(input_filename).flatMap(lambda x: x.split(" ")).map(lambda x: (x, 1)).reduceByKey(lambda a, b: a + b)
    rdd = R.TextFile("inputfile4.txt").flatMap(lambda x: x.split(" ")).map(lambda x: (x, 1)).reduceByKey_Hash(lambda a, b: a + b)


    rdd = R.TextFile("inputfile4.txt").flatMap(lambda x: x.split(" ")).map(lambda x: (x, 1)).mapValues(lambda rank: rank * 0.85 + 0.15)


    #rdd2 = rdd.filter(lambda x:int(x[1])>1).count()
    print rdd.collect()



