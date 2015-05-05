__author__ = 'pengzhan'
# python mr_job.py 127.0.0.1:4242 wordcount 100000 4 book.txt count
import zerorpc
import sys
from myRDD import *
import StringIO
import cloudpickle


if __name__ == '__main__':
    input_filename = sys.argv[1]
    master_addr = sys.argv[2]
    c = zerorpc.Client()
    c.connect("tcp://"+master_addr)
    R = RDD()
    rdd = R.TextFile(input_filename).flatMap(lambda x: x.split(" ")).map(lambda x: (x, 1)).reduceByKey(lambda a, b: a + b)

    output = StringIO.StringIO()
    pickler = cloudpickle.CloudPickler(output)
    pickler.dump(rdd)
    pickle_object = output.getvalue()

    c.set_job(pickle_object)