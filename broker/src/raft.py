from typing import Dict, List, Union

from pysyncobj import SyncObj, replicated
from pysyncobj.batteries import ReplCounter, ReplDict

MSG_ALL_REPLICA_SET_BIT = (1<<3) - 1

class PartitionRaft(SyncObj):
    def __init__(self, raft_host:str, raft_partners:List[str], topic_name:str, partition_id:int):
        super(PartitionRaft, self).__init__(raft_host, raft_partners)
        self.topic_name = topic_name
        self.partition_id = partition_id
        self.msg_count = 0
        self.msg_dict = {}
        self.consumer_dict = {}

    @replicated
    def add_message(self, msg_id:int, msg: str) -> None:
        self.msg_count += 1
        self.msg_dict[msg_id] = [msg, MSG_ALL_REPLICA_SET_BIT]

    @replicated
    def add_consumer(self, consumer_id: int) -> None:
        self.consumer_dict[consumer_id] = 0

    
    def get_raft_status(self) -> Dict:
        status = self.getStatus()
        status['self'] = status['self'].address

        if status['leader']:
            status['leader'] = status['leader'].address

        serializable_status = {
            **status,
            'is_leader': status['self'] == status['leader'],
        }
        return serializable_status


    def is_leader(self) -> bool:
        return self.get_raft_status().get('is_leader', False)

    

# SYNC_OBJ: SyncObj = None

# MSG_COUNT = ReplCounter()
# DATA_DICT = ReplDict()
# CONSUMER_DICT = ReplDict()


# def create_sync_obj(raft_host: str, partners: List[str]):
#     global SYNC_OBJ
#     if SYNC_OBJ:
#         return

#     SYNC_OBJ = SyncObj(raft_host, partners, consumers=get_distributed_objs())
#     SYNC_OBJ.waitBinded()
#     SYNC_OBJ.waitReady()


# def get_sync_obj() -> Union[SyncObj, None]:
#     global SYNC_OBJ
#     if SYNC_OBJ:
#         return SYNC_OBJ

#     raise Exception('Sync object is not created')

# def get_msg_cnt() -> ReplDict:
#     return MSG_COUNT

# def get_data_dict() -> ReplDict:
#     return DATA_DICT

# def get_consumer_dict() -> ReplDict:
#     return CONSUMER_DICT

# def get_distributed_objs() -> List:
#     return [get_msg_cnt(), get_data_dict(), get_consumer_dict()]

# def get_raft_status() -> Dict:
#     status = get_sync_obj().getStatus()
#     status['self'] = status['self'].address

#     if status['leader']:
#         status['leader'] = status['leader'].address

#     serializable_status = {
#         **status,
#         'is_leader': status['self'] == status['leader'],
#     }
#     return serializable_status


# def is_leader() -> bool:
#     return get_raft_status().get('is_leader', False)


# def add_broker(id:int, broker_addr: str) -> bool:
#     pass