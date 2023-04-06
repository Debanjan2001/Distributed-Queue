import requests
import json
import sys
import subprocess

try:
    port = sys.argv[1]
    topic_name = sys.argv[2]
    partition_id = int(sys.argv[3])
    consumer_id = sys.argv[4]
    host_port = sys.argv[5]
    partner1_port = sys.argv[6]
    partner2_port = sys.argv[7]
except:
    port = input("Port: ")
    topic_name = input("Topic Name: ")
    partition_id = int(input("Partition_id: "))
    consumer_id = input("Consumer ID: ")
    host_port = input("Host Port: ")
    partner1_port = input("Partner1 Port: ")
    partner2_port = input("Partner2 Port: ")

broker_url = f"http://127.0.0.1:{port}"

subprocess.run(["python", "test_partition_add.py", port, topic_name, str(partition_id), host_port, partner1_port, partner2_port])

resp = requests.post(
    broker_url + "/consumers",
    json={
        "topic_name": topic_name,
        "partition_id": partition_id,
        "consumer_id": consumer_id,
    })

print(25*"-")
print("Requesting add Consumer")
print(f"Status Code = {resp.status_code}")
print(resp.json())
print(25*"-")