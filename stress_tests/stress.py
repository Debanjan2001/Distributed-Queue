import requests
import json
import sys

NUM_TOPICS = 6
NUM_PROUDUCERS_PER_TOPIC = 3
NUM_CONSUMERS_PER_TOPIC = 3
MESSAGE_PER_PRODUCER = 3
NUM_PARTITIONS = 3

print(50*"#")
print("Testing Add Broker")

manager_url = f"http://127.0.0.1:4000"

broker_urls = ["http://127.0.0.1:8000", "http://127.0.0.1:8001", "http://127.0.0.1:8002"]


for broker_url in broker_urls:
    resp = requests.post(
        url=manager_url + "/brokers",
        json={
            "broker_addr": broker_url
        }
    )
    print(25*"-")
    print("adding broker -", broker_url)
    print(25*"-")

print(25*"-")
print(f"adding {NUM_TOPICS} topics")
producer_ids = {}
consumer_ids = {}
for num in range(NUM_TOPICS):
    resp = requests.post(
        url = manager_url + "/topics",
        json={
            "topic_name": f"T{num}"
        }
    )
    producer_ids[f"T{num}"] = []
    consumer_ids[f"T{num}"] = []

print(25*"-")
print("adding producer")
for topic_num in range(NUM_TOPICS):
    for _ in range(NUM_PROUDUCERS_PER_TOPIC):
        
        resp = requests.post(
            url = manager_url + "/producers",
            json={
                "topic_name": f"T{topic_num}"
            }
        )
        producer_id = resp.json()["producer_id"]
        producer_ids[f"T{topic_num}"].append(producer_id)
print(25*"-")

print(25*"-")
print("adding partitions") 
for topic_num in range(NUM_TOPICS):
    for partition_num in range(1, NUM_PARTITIONS):
        producer_id = producer_ids[f"T{topic_num}"][partition_num % NUM_PROUDUCERS_PER_TOPIC]
        resp = requests.post(
            url = manager_url + "/partitions",
            json={
                "topic_name": f"T{topic_num}",
                "producer_id": producer_id
            }
        )


print(25*"-")
print("adding message")
for topic_num in range(NUM_TOPICS):
    for producer_id in producer_ids[f"T{topic_num}"]:
        for message_num in range(MESSAGE_PER_PRODUCER):
            resp = requests.post(
                url = manager_url + "/messages",
                json={
                    "producer_id": producer_id,
                    "topic_name": f"T{topic_num}",
                    "partition_id": message_num % NUM_PARTITIONS,
                    "message": f"{message_num}th message for Topic T{topic_num} by producer P{producer_id}",
                }
            )
            assert(resp.status_code == 201)
print(25*"-")

print(25*"-")
print("adding consumer")
for topic_num in range(NUM_TOPICS):
    for _ in range(NUM_CONSUMERS_PER_TOPIC):
        
        resp = requests.post(
            url = manager_url + "/consumers",
            json={
                "topic_name": f"T{topic_num}"
            }
        )
        consumer_id = resp.json()["consumer_id"]
        consumer_ids[f"T{topic_num}"].append(consumer_id)
print(25*"-")


print(25*"-")
print("reading messages")
for topic_num in range(NUM_TOPICS):
    for consumer_id in consumer_ids[f"T{topic_num}"]:
        for producer_id in producer_ids[f"T{topic_num}"]:
            for message_num in range(MESSAGE_PER_PRODUCER):
                resp = requests.get(
                    url = manager_url + "/messages",
                    json={
                        "consumer_id": consumer_id,
                        "topic_name": f"T{topic_num}",
                        "partition_id": message_num % NUM_PARTITIONS,
                    }
                )
                assert(resp.status_code == 200)
                with open('consumer_outs/T{}_C{}.txt'.format(topic_num, consumer_id), 'a') as f:
                    f.write(resp.json()["message"] + "\n")
print(25*"-")

print(50*"#")
