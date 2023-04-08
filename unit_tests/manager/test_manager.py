import requests
import json
import sys

print(50*"#")
print("Testing Add Broker")

manager_url = f"http://127.0.0.1:5000"

broker_urls = ["http://127.0.0.1:8000", "http://127.0.0.1:8001", "http://127.0.0.1:8002"]

for broker_url in broker_urls:
    resp = requests.post(
        url=manager_url + "/brokers",
        json={
            "broker_addr": broker_url
        }
    )
    print(25*"-")
    print("adding broker")
    print(resp.json())
    print(25*"-")

print(25*"-")
print("adding topic")
resp = requests.post(
    url = manager_url + "/topics",
    json={
        "topic_name": "T1"
    }
)
print(resp.json())
print(25*"-")


print(25*"-")
print("adding producer")
resp = requests.post(
    url = manager_url + "/producers",
    json={
        "topic_name": "T1"
    }
)
print(resp.json())
producer_id = resp.json()["producer_id"]
print(25*"-")


print(25*"-")
print("adding message")
resp = requests.post(
    url = manager_url + "/messages",
    json={
        "producer_id": producer_id,
        "topic_name": "T1",
        "partition_id": 0,
        "message": "1st message for Topic T1",
    }
)
print(resp.json())
print(25*"-")

print(25*"-")
print("adding consumer")
resp = requests.post(
    url = manager_url + "/consumers",
    json={
        "topic_name": "T1",
    }
)
print(resp.json())
consumer_id = resp.json()["consumer_id"]
print(25*"-")


print(25*"-")
print("reading messages")
for i in range(2):
    print("*****")
    print(f"Query ==> {i+1}")
    print("*****")
    resp = requests.get(
        url = manager_url + "/messages",
        json={
            "consumer_id": consumer_id,
            "topic_name": "T1",
            "partition_id": 0,
        }
    )
    print(resp.json())
print(25*"-")

print(50*"#")
