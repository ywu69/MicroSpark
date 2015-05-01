__author__ = 'pengzhan'
# python mr_job.py 127.0.0.1:4242 wordcount 100000 4 book.txt count
import zerorpc
import sys
if __name__ == '__main__':
    master_addr = sys.argv[1]
    job_name = sys.argv[2]
    split_size = sys.argv[3]
    num_reducers = sys.argv[4]
    input_filename = sys.argv[5]
    output_filename_base = sys.argv[6]
    c = zerorpc.Client()
    c.connect("tcp://"+master_addr)
    c.set_job(job_name, split_size, num_reducers, input_filename, output_filename_base)