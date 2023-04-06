from typing import Dict, List, Union
import uuid

from pysyncobj import SyncObj
from pysyncobj.batteries import ReplDict

SYNC_OBJ: SyncObj = None

PRODUCER_DICT = ReplDict()
CONSUMER_DICT = ReplDict()
TOPIC_DICT = ReplDict()
BROKER_DICT = ReplDict()

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

def get_distributed_objs() -> List[ReplDict]:
    return [get_producer_dict(), get_consumer_dict(), get_topic_dict(), get_broker_dict()]

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


def add_broker(broker_addr: str) -> bool:
    broker_dict = get_broker_dict()
    if broker_addr in broker_dict.values():
        raise Exception(f'Broker with id {broker_addr} already exists')
    
    broker_id = str(uuid.uuid4().hex)
    broker_dict[broker_id] = broker_addr

# Functions for topic_dict
def add_topic(topic_name: str):
    topic_dict = get_topic_dict()
    
    if topic_name in topic_dict:
        raise Exception(f'Topic {topic_name} already exists')
    
    topic_dict[topic_name] = {}

def add_broker_to_topic_partition(topic_name: str, partition_id: int, broker_id: str):
    topic_dict = get_topic_dict()

    if topic_name not in topic_dict:
        raise Exception(f'Topic {topic_name} does not exist')
    
    if partition_id not in topic_dict[partition_id]:
        topic_dict[topic_name][partition_id] = set()

    if broker_id in topic_dict[topic_name][partition_id]:
        raise Exception(f'Broker id {broker_id} already added for topic name {topic_name}.')
    
    topic_dict[topic_name][partition_id].add(broker_id)

def get_brokers_for_topic_partition(topic_name: str, partition_id: int):
    topic_dict = get_topic_dict()

    if topic_name not in topic_dict or partition_id not in topic_dict[partition_id]:
        return None

    broker_dict = get_broker_dict()
    return [broker_dict[broker_id] for broker_id in topic_dict[topic_name][partition_id]]

# Functions for producer_dict
def add_producer(topic_name: str) -> str:
    topic_dict = get_topic_dict()

    if topic_name not in topic_dict:
        add_topic(topic_name)

    producer_dict = get_producer_dict()

    producer_id = str(uuid.uuid4().hex)
    producer_dict[producer_id] = topic_name

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

    consumer_id = str(uuid.uuid4().hex)
    consumer_dict[consumer_id] = topic_name
    return consumer_id

def is_consumer(consumer_id: str, topic_name: str) -> bool:
    consumer_dict = get_consumer_dict()

    if consumer_id not in consumer_dict or consumer_dict[consumer_id] != topic_name:
        return False    
    return True

