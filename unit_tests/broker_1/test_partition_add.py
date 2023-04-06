import requests
import json
import subprocess
import sys

try:
    port = sys.argv[1]
    topic_name = sys.argv[2]
    partition_id = int(sys.argv[3])
    host_port = sys.argv[4]
    partner1_port = sys.argv[5]
    partner2_port = sys.argv[6]
except:
    port = input("Port: ")
    topic_name = input("Topic Name: ")
    partition_id = int(input("Partition_id: "))
    host_port = input("Host Port: ")
    partner1_port = input("Partner1 Port: ")
    partner2_port = input("Partner2 Port: ")

broker_url = f"http://127.0.0.1:{port}"

subprocess.run(["python", "test_heartbeat.py", port])
    
resp = requests.post(
    broker_url + "/partitions",
    json={
        "topic_name": topic_name,
        "partition_id": partition_id,
        "replica_id": 1,
        "raft_host": f"localhost:{host_port}",
        "raft_partners": [f"localhost:{partner1_port}", f"localhost:{partner2_port}"]
    })

print(25*"-")
print("Requesting add Partition")
print(f"Status Code = {resp.status_code}")
print(resp.json())
print(25*"-")

print("Querying all partitions")
resp = requests.get(broker_url + "/partitions")
print(25*"-")
print("Requesting get Partition")
print(f"Status Code = {resp.status_code}")
print(resp.json())
print(25*"-")
