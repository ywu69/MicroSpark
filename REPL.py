__author__ = 'blu2'
import sys
import code
from gevent import fileobject
import gevent
import sys

_green_stdin = fileobject.FileObject(sys.stdin)
_green_stdout = fileobject.FileObject(sys.stdout)






def _green_raw_input(prompt):
    _green_stdout.write(prompt)
    line = _green_stdin.readline()[:-1]
    if line == "quit()":
        sys.exit(1)
    return line

def run_console(prompt=">>>"):
    import zerorpc
    import sys
    from myRDD import RDD
    import StringIO
    import cloudpickle
    from datetime import datetime
    import params
    from Context import Context

    # use exception trick to pick up the current frame
    try:
        raise None
    except:
        frame = sys.exc_info()[2].tb_frame.f_back

    # evaluate commands in current namespace
    namespace = frame.f_globals.copy()
    namespace.update(frame.f_locals)

    # C = Context()
    # #R = C.init()
    # R = RDD(None, None, C.getMasterAddress())
    # rdd = R.TextFile("/Users/blu2/Documents/Github/MicroSpark/inputfile4.txt").flatMap(lambda x: x.split(" ")).map(lambda x: (x, 1)).reduceByKey_Hash(lambda a, b: a + b)
    # rdd.collect()

    a = 3
    print "1"

    code.interact(prompt, _green_raw_input, local=namespace)



# if __name__ == "__main__":
#     run_console()



def keyboard(banner=None):
    import code, sys

    # use exception trick to pick up the current frame
    try:
        raise None
    except:
        frame = sys.exc_info()[2].tb_frame.f_back

    # evaluate commands in current namespace
    namespace = frame.f_globals.copy()
    namespace.update(frame.f_locals)

    code.interact(banner=banner, local=namespace)

def func():
    import zerorpc
    import sys
    from myRDD import RDD
    import StringIO
    import cloudpickle
    from datetime import datetime
    import params
    from Context import Context
    C = Context()
    #R = C.init()
    R = RDD(None, None, C.getMasterAddress())
    rdd = R.TextFile("/Users/blu2/Documents/Github/MicroSpark/inputfile4.txt").flatMap(lambda x: x.split(" ")).map(lambda x: (x, 1)).reduceByKey_Hash(lambda a, b: a + b).count()
    rdd.collect()


    keyboard()


func()