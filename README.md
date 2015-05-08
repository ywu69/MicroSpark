# MicroSpark
Implement a MicroSpark framework on cloud


Usage:

1. Master
    python mr_master.py 4242
2. Worker
    python mr_worker.py 10000 1 localhost:4242   (Normal)
    python mr_worker.py 10000 2 localhost:4242   (Standby)
3. Driver
    python mr_job.py myfile localhost:4242