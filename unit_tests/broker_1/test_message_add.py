# import requests
# import json
# import sys
# import subprocess

# try:
#     port = sys.argv[1]
#     topic_name = sys.argv[2]
#     partition_id = int(sys.argv[3])
#     consumer_id = sys.argv[4]
# except:
#     port = input("Port: ")
#     topic_name = input("Topic Name: ")
#     partition_id = int(input("Partition_id: "))
#     consumer_id = input("Consumer_id: ")

# broker_url = f"http://127.0.0.1:{port}"

# subprocess.run(["python", "test_partition_add.py", topic_name, str(partition_id)])
# subprocess.run(["python", "test_consumer_add.py", topic_name, str(partition_id), consumer_id])

# # resp = requests.post(
# #     broker_url + "/partitions",
# #     json={
# #         "topic_name": topic_name,
# #         "partition_id": partition_id,
# #         "replica_id": 1,
# #         "raft_host": "localhost:10050",
# #         "raft_partners": ["localhost:10051", "localhost:10052"]
# #     })
# print(f"Status Code = {resp.status_code}")
# print(25*"-")
# print(resp.json())
# print(25*"-")

# print("Querying all partitions")
# resp = requests.get(broker_url + "/partitions")
# print(f"Status Code = {resp.status_code}")
# print(25*"-")
# print(resp.json())
# print(25*"-")
