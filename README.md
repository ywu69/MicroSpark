# MicroSpark
Implement a MicroSpark framework on cloud


Usage:<br>

Local:<br>
1. Master<br>
    python mr_master.py 4242<br>
2. Worker
    python mr_worker.py 10000 1 localhost:4242   (Normal)<br>
    python mr_worker.py 10000 2 localhost:4242   (Standby) <br>
3. Driver<br>
    python mr_job.py myfile localhost:4242<br>



Cluster:

ssh stargate.cs.usfca.edu

1Master:<br>
python mr_master.py 10000<br>
<br>
2Worker:<br>
python mr_worker.py 10000 1 bass07.cs.usfca.edu:10000<br>
python mr_worker.py 10000 2 bass07.cs.usfca.edu:10000<br>
<br>
3.Client:<br>
(1)WordCount: <br>python wordcount.py inputfile.txt bass07.cs.usfca.edu:10000<br>
(2)PageRank: <br>python pagerank.py pagerank bass07.cs.usfca.edu:10000<br>
(3)REPL:python REPL.py bass07.cs.usfca.edu:10000<br>
rdd = R.TextFile("/home/blu2/cs636/MicroSpark/log.txt").flatMap(lambda x: x.split('\n')).filter(lambda x: x.startswith('ERROR'))
<br>
(4)failures
(5)Driver
<br>




