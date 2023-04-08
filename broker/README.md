# Broker


## Sample Commands to Run the manager
```
source setup_db.sh broker_1 && python3 run.py --flask_host 0.0.0.0:8000
source setup_db.sh broker_2 && python3 run.py --flask_host 0.0.0.0:8001
source setup_db.sh broker_3 && python3 run.py --flask_host 0.0.0.0:8002
```

## ToDo/Thoughts of where code can break
- [x] Replica management: set replica_id in the value of partition_dict 
- [x] Delete comments and cluttered code.
- [x] consumer_id should be str type everywhere.
- [x] Add replica_id, thread to partitionDict and erase them when sigint is sent
- [] As of now, just calling a function which has @replicated decorator, might not function due to timeouts, what happens in those cases? is there an exception raised or the message is dropped silently?
- [x] Replicated Method can't return any value, hence we might have to see which iteration we should use for the PartitionRaft abstraction
- [] Adding Database


### Command to setup db for first time
```
source setup_db.sh broker_1
```

- Then use run.py to start the app
