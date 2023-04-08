# Broker Unit tests

- Replica_id has been set to `host_port%3` for simplicity, hence only use the following commands for fault-safe tests

## Sample Commands to test the Broker

### Heartbeat Test
```
python3 test_heartbeat.py 8000
python3 test_heartbeat.py 8001
python3 test_heartbeat.py 8002
```

### Partition Addition Test
```
python3 test_partition_add.py 8000 t1 1 9000 9001 9002
python3 test_partition_add.py 8001 t1 1 9001 9000 9002
python3 test_partition_add.py 8002 t1 1 9002 9000 9001
```

### Partition Raft Status Test
```
python3 test_partition_raft_status.py 8000 t1 1 9000 9001 9002
python3 test_partition_raft_status.py 8001 t1 1 9001 9000 9002
python3 test_partition_raft_status.py 8002 t1 1 9002 9000 9001

```

### Partition Consumer Add Test
```
python3 test_consumer_add.py 8000 t1 1 10 9000 9001 9002
python3 test_consumer_add.py 8001 t1 1 11 9001 9000 9002
python3 test_consumer_add.py 8002 t1 1 12 9002 9000 9001
```

### Partition Add Message Test
```
python3 test_message_add.py 8000 t1 1 9000 9001 9002 test_message1 
```

### Partition Get Message Test
```
python3 test_message_get.py 8001 t1 1 10 9001 9000 9002 
```

