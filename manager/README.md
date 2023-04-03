# Manager

## Sample Commands to Run the manager
```
python3 run.py --flask_host localhost:5000 --raft_host localhost:6000 --partners localhost:6001 localhost:6002
python3 run.py --flask_host localhost:5001 --raft_host localhost:6001 --partners localhost:6000 localhost:6002
python3 run.py --flask_host localhost:5002 --raft_host localhost:6002 --partners localhost:6001 localhost:6000
```