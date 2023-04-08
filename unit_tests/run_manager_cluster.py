from multiprocessing import Process
import os
def run_cmd(cmd):
    os.system(f'{cmd}')

cmds = [
    "python3 ../manager/run.py --flask_host localhost:5000 --raft_host localhost:6000 --partners localhost:6001 localhost:6002",
    "python3 ../manager/run.py --flask_host localhost:5001 --raft_host localhost:6001 --partners localhost:6000 localhost:6002",
    "python3 ../manager/run.py --flask_host localhost:5002 --raft_host localhost:6002 --partners localhost:6001 localhost:6000",
]


for cmd in cmds:
    p = Process(target=run_cmd, args=(cmd,))
    p.start()


import time
time.sleep(5)
