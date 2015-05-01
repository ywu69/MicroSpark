1. Start the master:

$ python mr_master.py <port> <data_dir>

2. Start the workers

$ python mr_worker.py <worker_port> <ip_address_master:port> 

3. Start a MapReduce job:

$ python mr_job.py <ip_address_master:port> <name> <split_size> <num_reducers> <input_filename> <output_filename_base>

And for sequential execution (no master or workers):

$ python mr_seq.py <name> <split_size> <num_reducers> <input_filename> <output_filename_base>

4. Collect results from workers:

$ python mr_collect.py <filename_base> <output_filename>
