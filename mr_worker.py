__author__ = 'pengzhan'
import zerorpc
import sys
import socket
import gevent

class Worker(object):
    def __init__(self, master_addr, worker_ip, worker_port):
        self.master_addr = master_addr
        self.worker_port = worker_port
        self.worker_ip = worker_ip
        self.c = zerorpc.Client()
        self.c.connect("tcp://"+master_addr)
        self.c.register(worker_ip, worker_port)

    def controller(self):
        while True:
            print('[Worker]')
            gevent.sleep(1)

    def ping(self):
        pass
        print('[Worker] Ping from Master')

if __name__ == '__main__':
    worker_ip = socket.gethostbyname(socket.gethostname())
    worker_port = '8000'#sys.argv[1]
    master_addr = '127.0.0.1:4242'#sys.argv[2];
    s = zerorpc.Server(Worker(master_addr,worker_ip,worker_port))
    s.bind('tcp://' + worker_ip+":"+worker_port)
    s.run()

    #w = Worker('A','B',1000)
    #chunk = (11,36)
    #w.do_map('a', 'inputfile2.txt', chunk)
    #print socket.gethostbyname(socket.gethostname())