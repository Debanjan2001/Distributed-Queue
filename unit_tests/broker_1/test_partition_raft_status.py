import requests
import json
import sys
import subprocess

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

subprocess.run(["python", "test_partition_add.py", port, topic_name, str(partition_id), host_port, partner1_port, partner2_port])

resp = requests.get(
    broker_url + "/raft_status",
    json={
        "topic_name": topic_name,
        "partition_id": partition_id,
    })

print(25*"-")
print("Requesting get Partition Raft Status")
print(f"Status Code = {resp.status_code}")
print(resp.json())
print(25*"-")