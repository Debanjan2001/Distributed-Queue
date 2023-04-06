import threading
from pysyncobj import SyncObj,SyncObjConf, replicated
from pysyncobj.batteries import ReplCounter, ReplDict
import time

def check_status(partition_raft):
    while(True):
        import time
        time.sleep(5)
        print(partition_raft.getStatus())

MSG_ALL_REPLICA_SET_BIT = (1<<3) - 1  # 3 replicas in the cluster

# class PartitionRaft(SyncObj):
#     def __init__(
#             self, 
#             raft_host:str, 
#             raft_partners:List[str], 
#             topic_name:str, 
#             partition_id:int,
#             replica_id:int
#         ):
#         super(PartitionRaft, self).__init__(raft_host, raft_partners)
#         self.topic_name = topic_name
#         self.partition_id = partition_id
#         # self.replica_id = replica_id
#         self.msg_count = 0
#         self.msg_dict = {}
#         self.consumer_dict = {}

#     @replicated
#     def add_message(self, msg: str) -> None:
#         self.msg_dict[self.msg_count] = [msg, MSG_ALL_REPLICA_SET_BIT]
#         self.msg_count += 1

#     @replicated
#     def get_message(self, consumer_id:int) -> Union[None, str]:
#         if msg_id not in self.msg_dict:
#             return None
#         if consumer_id not in self.consumer_dict:
#             return None
#         msg_id = self.consumer_dict[consumer_id] 
#         if msg_id >= self.msg_count:
#             return None
#         self.consumer_dict[consumer_id] += 1
#         return self.msg_dict[msg_id]

#     @replicated
#     def add_consumer(self, consumer_id: int) -> None:
#         print("yes")
#         print(self.consumer_dict)
#         if consumer_id in self.consumer_dict:
#             raise Exception("Consumer already registered")
#         self.consumer_dict[consumer_id] = 0

#     def get_consumer_offset(self, consumer_id: int) -> int:
#         if consumer_id not in self.consumer_dict:
#             raise Exception("Consumer not registered")
#         return self.consumer_dict[consumer_id]

#     def get_raft_status(self) -> Dict:
#         status = self.getStatus()
#         status['self'] = status['self'].address

#         if status['leader']:
#             status['leader'] = status['leader'].address

#         serializable_status = {
#             **status,
#             'is_leader': status['self'] == status['leader'],
#         }
#         return serializable_status


#     def is_leader(self) -> bool:
#         return self.get_raft_status().get('is_leader', False)


class PartitionRaft():
    def __init__(
            self, 
            raft_host, 
            raft_partners, 
            topic_name, 
            partition_id,
            replica_id
        ):
        self.topic_name = topic_name
        self.partition_id = partition_id
        # self.replica_id = replica_id
        self.msg_count = ReplCounter()
        self.msg_dict = ReplDict()
        self.consumer_dict = ReplDict()
        self.sync_obj = SyncObj(raft_host, raft_partners, consumers=[self.msg_count, self.msg_dict, self.consumer_dict])
        self.sync_obj.waitBinded()
        self.sync_obj.waitReady()

    def add_message(self, msg: str):
        self.msg_dict[self.msg_count] = [msg, MSG_ALL_REPLICA_SET_BIT]
        self.msg_count.inc(sync=False)

    def get_message(self, consumer_id):
        if msg_id not in self.msg_dict:
            return None
        if consumer_id not in self.consumer_dict:
            return None
        msg_id = self.consumer_dict[consumer_id] 
        if msg_id >= self.msg_count:
            return None
        self.consumer_dict[consumer_id].inc(sync=False)
        return self.msg_dict[msg_id]

    def add_consumer(self, consumer_id):
        print(self.consumer_dict)
        if consumer_id in self.consumer_dict:
            raise Exception("Consumer already registered")
        self.consumer_dict[consumer_id] = 0

    def get_consumer_offset(self, consumer_id):
        if consumer_id not in self.consumer_dict:
            raise Exception("Consumer not registered")
        return self.consumer_dict[consumer_id]

    def get_raft_status(self):
        status = self.sync_obj.getStatus()
        status['self'] = status['self'].address

        if status['leader']:
            status['leader'] = status['leader'].address

        serializable_status = {
            **status,
            'is_leader': status['self'] == status['leader'],
        }
        return serializable_status

    def is_leader(self):
        return self.sync_obj.get_raft_status().get('is_leader', False)

    
class PartitionDict:
    def __init__(self):
        self.partitions = {}
        self.lock = threading.Lock()

    def add_partition(self, partition: PartitionRaft):
        self.lock.acquire()
        self.partitions[(partition.topic_name, partition.partition_id)] = partition
        self.lock.release()

    def add_partition_inplace(self, raft_host:str, raft_partners:list, topic_name:str, partition_id:int, replica_id:int):
        self.lock.acquire()
        self.partitions[(topic_name, partition_id)] = PartitionRaft(raft_host, raft_partners, topic_name, partition_id, replica_id)
        # self.partitions[(topic_name, partition_id)].waitBinded()
        # self.partitions[(topic_name, partition_id)].waitReady()
        # while not self.partitions[(topic_name, partition_id)].isReady():
        #     print("waiting...")
        #     time.sleep(0.5)
        # time.sleep(5)
        self.lock.release()

    def get_partition(self, topic_name:str, partition_id: int):
        return self.partitions.get((topic_name, partition_id), None)

    def get_partitions(self):
        return list(self.partitions.values())
    
    def get_keys(self):
        return list(self.partitions.keys())