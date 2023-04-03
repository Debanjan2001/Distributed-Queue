from typing import Dict, List, Union

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


def add_broker(id:int, broker_addr: str) -> bool:
    broker_dict = get_broker_dict()
    if id in broker_dict:
        raise Exception(f'Broker with id {id} already exists')
    broker_dict[id] = broker_addr