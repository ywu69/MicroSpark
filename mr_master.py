__author__ = 'pengzhan'
import sys

import zerorpc
import os
import gevent
import socket
from gevent import timeout
import StringIO
import pickle
import cloudpickle
import copy

WORKER_NORMAL = "1"
WORKER_STANDBY = "2"

class Master(object):



    def __init__(self, data_dir):
        gevent.spawn(self.controller)
        self.state = 'READY'
        self.data_dir = data_dir

        self.workers = {}
        self.workers_standby = {}
        self.workerState = {}

        self.workerlist_for_RDD = {}

        self.current_intermediateRDD = None

    def controller(self):
        while True:
            print '[Master:%s] ' % (self.state),
            for w in self.workers:
                print "Worker_Nomal: ",
                print '(%s,%s,%s)' % (w[0], w[1], self.workerState[w]),
            for w in self.workers_standby:
                print 'Worker_Standby: (%s,%s,%s)' % (w[0], w[1], self.workerState[w])
            print
            gevent.sleep(1)

    def ping_worker(self, w, type):
        while True:
            if self.workerState[w] != "LOSS":
                try:
                    if type == WORKER_NORMAL:
                        self.workers[w].ping()
                    else:
                        self.workers_standby[w].ping()
                except Exception:
                    if type == WORKER_NORMAL:
                        self.workerState[w] = "LOSS"
                        print "lost connection"
                        print "old workers:" + str(self.workers)
                        # remove lost worker
                        self.workers.pop(w, None)
                        # select new worker from standby list
                        selected_worker = self.select_worker_from_standby()
                        # pop from standby list
                        c = self.workers_standby.pop(selected_worker, None)
                        if c is not None:
                            # add select_worker to worker list
                            self.workers[selected_worker] = c
                            self.workerState[selected_worker] = "READY"
                            print "new workers: " + str(self.workers)
                            # update workerlist_for_RDD
                            print "old workerlist:" + str(self.workerlist_for_RDD)
                            if self.workerlist_for_RDD is None or len(self.workerlist_for_RDD.keys()) is 0:
                                continue
                            # 1.pop loss worker with index value
                            index = self.workerlist_for_RDD.pop(w[0] + ":" + w[1])
                            # 2. assign select_worker with this index
                            self.workerlist_for_RDD[selected_worker[0] + ":" + selected_worker[1]] = index
                            print "new workerlist:" + str(self.workerlist_for_RDD)


                            self.current_intermediateRDD.set_params_recv(self.workerlist_for_RDD)
                            output = StringIO.StringIO()
                            pickler = cloudpickle.CloudPickler(output)
                            pickler.dump(self.current_intermediateRDD)
                            pickle_object = output.getvalue()

                            print "####" + str(self.current_intermediateRDD.workerlist)

                            self.set_job_for_single_worker(selected_worker, pickle_object)

                            self.update_RDD_workerlists()



                        else:
                            """
                            since there is no enough standby workers.
                            Master may need to cancel the job, and let driver knows
                            """
                            print "########################## should shut down"
                            pass
                        print "new workers:" + str(self.workers)
                        print "standby: " + str(self.workers_standby)
                    else:
                        pass
                        """
                            the loss of standby worker can be caused by
                            either worker is down,
                            or being removed from the list.
                        """
                    break
            else:
                break
            gevent.sleep(1)

    def register_async(self, ip, port, type):
        print '[Master:%s] ' % self.state,
        print 'Registered worker (%s,%s,%s)' % (ip, port, str(type))
        c = zerorpc.Client()
        print "Type: " + str(type)
        c.connect("tcp://" + ip + ':' + port)
        if type == WORKER_NORMAL:
            self.workers[(ip,port)] = c
        else:# type == WORKER_STANDBY
            self.workers_standby[(ip,port)] = c
            print "add: "+ str(self.workers_standby)
        self.workerState[(ip,port)] = 'READY'
        gevent.spawn(self.ping_worker,(ip,port), type)

        # #####---ONLY FOR TESTING---########
        # if len(self.workers) == 2:
        #     for w in self.workers:
        #         self.workers[w].cal()


    def register(self, ip, port, type):
        gevent.spawn(self.register_async, ip, port, type)

    def giveRDDWorkerInfo(self,rdd, worker_map, workerIndex):
        rdd.workerlist.clear()
        for e in worker_map:
            rdd.workerlist[e] = worker_map[e]
        while rdd != None:
            rdd.workerIndex = workerIndex
            rdd = rdd.prev

    def set_job_for_single_worker(self, w, pickle_object):
        gevent.spawn(self.setJob_async_for_single_worker, w, pickle_object)

    def setJob_async_for_single_worker(self, w, pickle_object):
        self.assign_rdd_to_worker(w, pickle_object)


    def set_job(self, pickle_object):
        gevent.spawn(self.setJob_async, pickle_object)

    def setJob_async(self, pickle_object):

        # unpickle
        input = StringIO.StringIO(pickle_object)
        unpickler = pickle.Unpickler(input)
        intermediateRDD = unpickler.load()

        input_filename = intermediateRDD.get_ancester().get_input_filename()

        workerlist = self.create_RDD_workerlist()

        print workerlist
        intermediateRDD.set_params_recv(workerlist)

        output = StringIO.StringIO()
        pickler = cloudpickle.CloudPickler(output)
        pickler.dump(intermediateRDD)
        pickle_object = output.getvalue()

        # save pickle_object for failure handling
        self.current_intermediateRDD = intermediateRDD

        for w in self.workers:
            self.assign_rdd_to_worker(w, pickle_object)

        print "in setjob_async"
        pass

    def create_RDD_workerlist(self):
        workerlist = {}
        i = 1
        for w in self.workers:
            workerlist[w[0] + ":" + w[1]] = i
            i += 1
        self.workerlist_for_RDD = copy.deepcopy(workerlist)
        return workerlist

    def assign_rdd_to_worker(self, w, pickle_object):
        gevent.spawn(self.assign_rdd_to_worker_async, w, pickle_object)

    def assign_rdd_to_worker_async(self, w, pickle_object):
        c = zerorpc.Client()
        print str(w[0]) + ":" + str(w[1])
        c.connect("tcp://" + w[0] + ":" + w[1])
        c.setRDD(pickle_object)

    def select_worker_from_standby(self):
        selected_worker = None
        for w in self.workers_standby:
            if self.workerState[w] == 'READY':
                selected_worker = w
                break
        return selected_worker

    def update_RDD_workerlists(self):
        print "try to update workerlists on :" +str(self.workers)
        for w in self.workers:
            c = zerorpc.Client()
            c.connect("tcp://" + w[0] + ":" + w[1])
            print "update_RDD_workerlists: connected to " + str(w[0]) + ":" + str(w[1])
            c.update_RDD_workerlist(self.workerlist_for_RDD)

    def set_worker_state(self, ip, port, state):
        gevent.spawn(self.set_worker_state_async, ip, port, state)

    def set_worker_state_async(self, ip, port, state):
        print 'set' + ip + ':' + port + ' state ' + state
        self.workerState[(ip, port)] = state

    def result_is_ready(self):
        done = False
        count = 0
        while done is False:
            for w in self.workers:
                if self.workerState[w] == "FINISHED":
                    print str(w) + " is finished"
                    count += 1
                else:
                    break

            if count == len(self.workers):
                print "#######################"
                # done, will break in next round
                done = True
            else:
                # reset count, await for a while
                count = 0
                gevent.sleep(1)

        #done
        worker_ips = []
        # current_workers = self.workerlist_for_RDD.keys()
        #
        # print "current_workers" + str(current_workers)

        for w in self.workers:
            worker_ips.append(w[0] + ":" + w[1])
        return worker_ips




if __name__ == '__main__':
    port = sys.argv[1]
    data_dir = '/'#sys.argv[2]
    if not os.path.exists(os.getcwd()+'/'+data_dir):
        print 'no such directory'
    else:
        master_addr = 'tcp://0.0.0.0:' + str(port)
        s = zerorpc.Server(Master(data_dir))
        s.bind(master_addr)
        s.run()
    #m = Master('/')
    #print m.split_file('inputfile3.txt', 16)
    #print socket.gethostbyname(socket.gethostname())