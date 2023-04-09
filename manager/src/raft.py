from typing import Dict, List, Union, Any
import uuid
import random

from pysyncobj import SyncObj
from pysyncobj.batteries import ReplDict, ReplCounter

import requests
from multiprocessing import Process
import os

SYNC_OBJ: SyncObj = None

PRODUCER_DICT = ReplDict()
CONSUMER_DICT = ReplDict()
TOPIC_DICT = ReplDict()
BROKER_DICT = ReplDict()
BASE_PORT = ReplCounter()

def create_sync_obj(raft_host: str, partners: List[str]):
    global SYNC_OBJ
    if SYNC_OBJ:
        return 
    SYNC_OBJ = SyncObj(raft_host, partners, consumers=get_distributed_objs())
    SYNC_OBJ.waitBinded()
    SYNC_OBJ.waitReady()


def get_sync_obj() -> Union[SyncObj, None]:
    global SYNC_OBJ
    if SYNC_OBJ:
        return SYNC_OBJ

    raise Exception('Sync object is not created')

def get_producer_dict() -> ReplDict:
    return PRODUCER_DICT

def get_consumer_dict() -> ReplDict:
    return CONSUMER_DICT

def get_topic_dict() -> ReplDict:
    return TOPIC_DICT

def get_broker_dict() -> ReplDict:
    return BROKER_DICT

def get_base_port() -> ReplCounter:
    return BASE_PORT

def get_distributed_objs() -> List[ReplDict]:
    return [get_producer_dict(), get_consumer_dict(), get_topic_dict(), get_broker_dict(), get_base_port()]

def get_raft_status() -> Dict:
    status = get_sync_obj().getStatus()
    status['self'] = status['self'].address

    if status['leader']:
        status['leader'] = status['leader'].address

    serializable_status = {
        **status,
        'is_leader': status['self'] == status['leader'],
    }
    return serializable_status


def is_leader() -> bool:
    return get_raft_status().get('is_leader', False)

def get_free_hosts(k: int, base_url: str = 'localhost'):
    base_port = get_base_port()
    
    hosts = []
    PORT_OFFSET = 10000
    for _ in range(k):
        host = base_url + ':' + str(PORT_OFFSET + base_port.inc(sync=True))
        hosts.append(host)
    
    return hosts

def add_broker(broker_addr: str) -> bool:
    broker_dict = get_broker_dict()
    if broker_addr in broker_dict.values():
        raise Exception(f'Broker with addr {broker_addr} already exists')
    
    broker_id = str(uuid.uuid4().hex)
    broker_dict.set(broker_id, broker_addr, sync=True)

def get_broker(broker_id: str) -> str:
    broker_dict = get_broker_dict()

    broker_addr = broker_dict.get(broker_id, None)
    if broker_addr is None:
        raise Exception(f'Broker with id {broker_id} does not exist')
    
    return broker_addr

def get_random_brokers(k: int):
    broker_dict = get_broker_dict()

    if k > len(broker_dict):
        raise Exception(f'Not enough brokers in the systems')
    
    broker_ids = list(broker_dict.keys())
    random.shuffle(broker_ids)

    return broker_ids[:k]

def run_broker(host: str):
    port = int(host.rsplit(':', 1)[1])
    print(port)
    os.system(f'/bin/bash -c "source ../broker/setup_db.sh broker_{port} && python3 ../broker/run.py --flask_host {host}"')

# Functions for topic_dict
def add_topic(topic_name: str):
    topic_dict = get_topic_dict()

    # if(len(topic_dict) % 3 == 0):
    #     # Spawn three new brokers
    #     try:
    #         broker_hosts = get_free_hosts(3, '127.0.0.1')
    #         for host in broker_hosts:
    #             p = Process(target=run_broker, args=(host,))
    #             p.start()
    #             add_broker(host)
    #         import time
    #         time.sleep(3)
    #     except Exception as e:
    #         print(f"[Broker Manager] add_topic(): {e}") 

    print(f"[Broker Manager] add_topic(): Trying to add topic {topic_name}")
    
    if topic_name in topic_dict:
        raise Exception(f'Topic {topic_name} already exists')
    
    topic_dict.set(topic_name, dict(), sync=True)
    # topic_dict[topic_name] = {}
    print(f"[Broker Manager] add_topic(): Adding {topic_name} to dictionary")

    # Initialize topic with 1 partition
    add_partition(topic_name)

def add_partition(topic_name: str):
    topic_dict = get_topic_dict()

    if topic_name not in topic_dict:
        raise Exception(f'Topic {topic_name} does not exist')

    print(f"[Broker Manager] add_partition(): Inside")
    
    partition_id = len(topic_dict.get(topic_name))

    # Review this assertion
    assert(partition_id not in topic_dict[topic_name])

    print(f"[Broker Manager] add_partition(): Adding partition {partition_id} to {topic_name}")

    partition_dict = topic_dict.get(topic_name)
    partition_dict[partition_id] = set()
    topic_dict.set(topic_name, partition_dict, sync=True)

    print(topic_dict.rawData())

    # @todo: Pick 3 brokers and corresponding 3 free hosts to add partitions replicas to.
    try:
        broker_ids = get_random_brokers(3)
        raft_hosts = get_free_hosts(3)
    except Exception as e:
        # print(str(e))
        raise Exception("Not enough brokers or free ports to ensure replication of partition")
    
    print(f"[Broker Manager] add_partition(): Found these 3 free hosts for {raft_hosts}: {topic_name}")
    
    for replica_id, broker_id in enumerate(broker_ids):
        add_broker_to_topic_partition(topic_name, partition_id, broker_id, replica_id, raft_hosts)

# This should ideally be only called by add_partition
def add_broker_to_topic_partition(topic_name: str, partition_id: int, broker_id: str, replica_id: int, raft_hosts: List[Any]):
    topic_dict = get_topic_dict()
    
    print(f"[Broker Manager] add_broker_to_topic_partition(): Inside")


    if topic_name not in topic_dict or partition_id not in topic_dict.get(topic_name):
        raise Exception(f'Topic {topic_name} or partition_id{partition_id} does not exist')
    
    ''' 
    Maybe used if we initialise from db for something
    '''
    # if partition_id not in topic_dict[partition_id]:
    #     add_partition(topic_name)
    #     topic_dict[topic_name][partition_id] = set()

    print(f"[Broker Manager] add_broker_to_topic_partition(): Flag 2")
    if broker_id in topic_dict[topic_name][partition_id]:
        raise Exception(f'Broker id {broker_id} already added for topic name {topic_name}.')
    
    partition_dict = topic_dict.get(topic_name)
    partition_dict[partition_id].add(broker_id)
    topic_dict.set(topic_name, partition_dict, sync=True)

    try:
        broker_addr = get_broker(broker_id)
    except Exception as e:
        raise(f"[Broker Manager] add_broker_to_topic_partition(): Unable to get addr for broker_id {broker_id}")
    
    # @todo: Pass raft port and it's neighbours to broker from some pool, make API call to broker
    # to initialize a partition replica at this port.
    # print(requests.get(broker_addr + '/').json())
    response = requests.post(
        url = broker_addr + "/partitions",
        json = {
            'topic_name': topic_name,
            'partition_id': partition_id,
            'replica_id': replica_id,
            'raft_host': raft_hosts[replica_id],
            'raft_partners': [raft_hosts[idx] for idx in range(len(raft_hosts)) if idx != replica_id]
        }
    )

    return response


def get_brokers_for_topic_partition(topic_name: str, partition_id: int):
    topic_dict = get_topic_dict()

    if topic_name not in topic_dict or partition_id not in topic_dict.get(topic_name):
        return None

    broker_dict = get_broker_dict()
    return [broker_dict.get(broker_id) for broker_id in topic_dict[topic_name][partition_id]]

# Functions for producer_dict
def add_producer(topic_name: str) -> str:
    topic_dict = get_topic_dict()

    print(f"[Broker Manager] add_producer(): Checking existence of {topic_name}")

    if topic_name not in topic_dict:
        add_topic(topic_name)

    producer_dict = get_producer_dict()

    producer_id = str(uuid.uuid4().hex)
    print(f"[Broker Manager] add_producer(): Adding producer id {producer_id} to {topic_name}")
    producer_dict.set(producer_id, topic_name, sync = True)

    print(f"[Broker Manager] add_producer()", producer_dict.rawData())

    return producer_id

def is_producer(producer_id: str, topic_name: str) -> bool:
    producer_dict = get_producer_dict()

    if producer_id not in producer_dict or producer_dict[producer_id] != topic_name:
        return False    
    return True

# Functions for consumer_dict
def add_consumer(topic_name: str) -> str:
    topic_dict = get_topic_dict()

    if topic_name not in topic_dict:
        raise Exception(f'Topic {topic_name} does not exist')

    consumer_dict = get_consumer_dict()
    partition_dict = topic_dict.get(topic_name)
    partition_ids = list(partition_dict.keys())
    consumer_id = str(uuid.uuid4().hex)
    print(f"[Broker Manager] add_consumer(): Adding consumer id {consumer_id} to {topic_name}")
    consumer_dict.set(consumer_id, topic_name, sync = True)

    print(f"[Broker Manager] add_consumer()", consumer_dict.rawData())
    return consumer_id, partition_ids

def is_consumer(consumer_id: str, topic_name: str) -> bool:
    consumer_dict = get_consumer_dict()

    if consumer_id not in consumer_dict or consumer_dict[consumer_id] != topic_name:
        return False    
    return True

