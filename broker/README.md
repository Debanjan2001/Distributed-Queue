# Broker


## Sample Commands to Run the manager
```
python3 run.py --flask_host 0.0.0.0:8000
python3 run.py --flask_host 0.0.0.0:8001
python3 run.py --flask_host 0.0.0.0:8002
```

## ToDo/Thoughts of where code can break
1. Replica management: set replica_id in the value of partition_dict
2. Delete comments and cluttered code.
3. consumer_id should be str type everywhere.
4. Add replica_id, thread to partitionDict and erase them when sigint is sent
5. As of now, just calling a function which has @replicated decorator, might not function due to timeouts, what happens in those cases? is there an exception raised or the message is dropped silently?
6. Replicated Method can't return any value, hence we might have to see which iteration we should use for the PartitionRaft abstraction
7. Adding Database
