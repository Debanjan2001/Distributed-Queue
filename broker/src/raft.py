from typing import Dict, List, Union

from pysyncobj import SyncObj
from pysyncobj.batteries import ReplCounter, ReplDict

SYNC_OBJ: SyncObj = None

MSG_COUNT = ReplCounter()
DATA_DICT = ReplDict()
CONSUMER_DICT = ReplDict()

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

def get_msg_cnt() -> ReplDict:
    return MSG_COUNT

def get_data_dict() -> ReplDict:
    return DATA_DICT

def get_consumer_dict() -> ReplDict:
    return CONSUMER_DICT

def get_distributed_objs() -> List:
    return [get_msg_cnt(), get_data_dict(), get_consumer_dict()]

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
    pass