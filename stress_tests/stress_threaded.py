import requests
import json
import sys
from threading import Thread
from time import sleep
import os

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


def add_topic(topic_name):
    resp = requests.post(
        url = manager_url + "/topics",
        json={
            "topic_name": topic_name
        }
    )

print(25*"-")
print(f"adding {NUM_TOPICS} topics")
producer_ids = {}
consumer_ids = {}

topic_threads = []
for num in range(NUM_TOPICS):
    topic_name = f"T{num}"
    topic_thread = Thread(target=add_topic, args=(topic_name,))
    topic_thread.start()
    topic_threads.append(topic_thread)
    producer_ids[topic_name] = []
    consumer_ids[topic_name] = []

for t in topic_threads:
    t.join()


print(25*"-")
print("adding producer")
for topic_num in range(NUM_TOPICS):
    for _ in range(NUM_PROUDUCERS_PER_TOPIC):
        try:
            resp = requests.post(
                url = manager_url + "/producers",
                json={
                    "topic_name": f"T{topic_num}"
                }
            )
            producer_id = resp.json()["producer_id"]
            producer_ids[f"T{topic_num}"].append(producer_id)
            sleep(0.1)
        except Exception as e:
            print(25*"-")
            print(e)
            print(resp.json())
            print(25*"-")
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



prod_cons_threads = []

def add_message(topic_num):
    for producer_id in producer_ids[f"T{topic_num}"]:
        for message_num in range(MESSAGE_PER_PRODUCER):
            try:
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
                sleep(0.1)
            except Exception as e:
                print(25*"-")
                print(e)
                print(resp.json())
                print(25*"-")

print(25*"-")
print("adding message")
for topic_num in range(NUM_TOPICS):
    t = Thread(target=add_message, args=(topic_num,))
    prod_cons_threads.append(t)
    t.start()
    
print(25*"-")

# sleep for some time to get new messages
sleep(0.5)
os.system("rm -rf consumer_outs/")
os.makedirs('consumer_outs/',exist_ok=True)

print(25*"-")
print("adding consumer")
for topic_num in range(NUM_TOPICS):
    for _ in range(NUM_CONSUMERS_PER_TOPIC):
        try:
            resp = requests.post(
                url = manager_url + "/consumers",
                json={
                    "topic_name": f"T{topic_num}"
                }
            )
            consumer_id = resp.json()["consumer_id"]
            consumer_ids[f"T{topic_num}"].append(consumer_id)
            sleep(0.1)
        except Exception as e:
            print(25*"-")
            print(e)
            print(resp.json())
            print(25*"-")
print(25*"-")


def read_message(topic_num):
    for consumer_id in consumer_ids[f"T{topic_num}"]:
        for producer_id in producer_ids[f"T{topic_num}"]:
            for message_num in range(MESSAGE_PER_PRODUCER):
                try:
                    resp = requests.get(
                        url = manager_url + "/messages",
                        json={
                            "consumer_id": consumer_id,
                            "topic_name": f"T{topic_num}",
                            "partition_id": message_num % NUM_PARTITIONS,
                        }
                    )
                    sleep(0.1)
                    assert(resp.status_code == 200)
                    with open('consumer_outs/T{}_C{}.txt'.format(topic_num, consumer_id), 'a') as f:
                        f.write(resp.json()["message"] + "\n")
                except Exception as e:
                    print(25*"-")
                    print(e)
                    print(resp.json())
                    print(25*"-")
                
print(25*"-")
print("reading messages")
for topic_num in range(NUM_TOPICS):
    t = Thread(target=read_message, args=(topic_num,))
    prod_cons_threads.append(t)
    t.start()
print(25*"-")

# wait for all threads to join
for t in prod_cons_threads:
    t.join()
    

print(50*"#")
