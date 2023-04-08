from typing import Dict, List, Union
import threading
from pysyncobj import SyncObj,SyncObjConf, replicated 
from pysyncobj.batteries import ReplCounter, ReplDict
from pysyncobj import SyncObjException
import time
import weakref
# from db_models.partition_raft import PartitionModel
# from db_models.log import LogModel

MSG_ALL_REPLICA_SET_BIT = (1<<3) - 1  # 3 replicas in the cluster

# Iteration-1 Model
# class PartitionRaft(SyncObj):
#     def __init__(
#             self, 
#             raft_host:str, 
#             raft_partners:List[str], 
#             topic_name:str, 
#             partition_id:int,
#         ):
#         super(PartitionRaft, self).__init__(
#             raft_host, 
#             raft_partners,
#             conf=SyncObjConf(
#                 autoTick=False
#             )
#         )
#         self.topic_name = topic_name
#         self.partition_id = partition_id
#         self.msg_count = 0
#         self.msg_dict = {}
#         self.consumer_dict = {}

#     @replicated(sync=True, timeout=2)
#     def add_message(self, msg: str) -> None:
#         self.msg_dict[self.msg_count] = [msg, MSG_ALL_REPLICA_SET_BIT]
#         self.msg_count += 1

#     # @replicated
#     # Replicated method can not return any value
#     def get_message(self, consumer_id) -> Union[None, str]:
#         msg_id = self.consumer_dict[consumer_id] 
#         if msg_id >= self.msg_count:
#             return None

#         # I am not sure whether this will work or not, but let's try testing
#         # self.consumer_dict[consumer_id] += 1
#         self.inc_consumer_offset(consumer_id)
#         return self.msg_dict[msg_id]

#     @replicated(sync=True, timeout=2)
#     def inc_consumer_offset(self, consumer_id):
#         self.consumer_dict[consumer_id] += 1


#     # replicated decorator doesn't allow returning values, hence needed the check_consumer method
#     # Raising exception in a @replicated method won't raise the exception to the caller
#     @replicated(sync=True, timeout=2)
#     def add_consumer(self, consumer_id) -> None:
#         # Checked this, it works
#         # print(self.consumer_dict)

#         if consumer_id in self.consumer_dict:
#             raise Exception("Consumer already registered")

#         self.consumer_dict[consumer_id] = 0
    
#     def has_consumer(self, consumer_id):
#         if consumer_id in self.consumer_dict:
#             return True
#         return False

#     def get_consumer_offset(self, consumer_id) -> int:
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
    
#     def remove(self):
#         self.destroy()

#     def is_leader(self) -> bool:
#         return self.get_raft_status().get('is_leader', False)
    
#     def __str__(self) -> str:
#         return f"PartitionRaft-{self.topic_name}-{self.partition_id}"
    
## End of iteration-1 Model


# Iteration-2 Model

class PartitionRaft():
    def __init__(
            self, 
            raft_host, 
            raft_partners, 
            topic_name, 
            partition_id,
        ):
        self.topic_name = topic_name
        self.partition_id = partition_id
        self.msg_count = ReplCounter()
        self.msg_dict = ReplDict()
        self.consumer_dict = ReplDict()
        # Timeout in seconds
        self.timeout = 3

        self.sync_obj = SyncObj(
            raft_host, 
            raft_partners, 
            consumers=[self.msg_count, self.msg_dict, self.consumer_dict],
            conf=SyncObjConf(
                autoTick=False
            )
        )

    def add_message(self, msg: str, msg_replicated_bitmask=MSG_ALL_REPLICA_SET_BIT):
        try:
            msg_id = self.msg_count.get()
            self.msg_dict.set(msg_id, [msg, msg_replicated_bitmask], sync=True)
            self.msg_count.inc(sync=True)
            return msg_id
        except SyncObjException:
            raise Exception("Unable to add message to the partition due to timeout")
        except Exception as e:
            raise e
        
    def unset_msg_replicated_bit(self, msg_id:int, replica_id:int):
        msg_object = self.msg_dict.get(msg_id)
        msg_object[1] ^= (1 << replica_id)
        if msg_object[1] == 0 : 
            self.msg_dict.pop(msg_id, sync=True)
        else:
            self.msg_dict.set(msg_id, msg_object, sync=True)
        
    def get_message(self, consumer_id):
        try:
            if consumer_id not in self.consumer_dict:
                self.add_consumer(consumer_id=consumer_id)
                # raise Exception(f"Consumer_id={consumer_id} not registered")
            
            msg_id = self.consumer_dict[consumer_id] 
            if msg_id >= self.msg_count.get():
                raise Exception(f"Consumer_id={consumer_id} has no new messages to be read")
            
            self.consumer_dict.set(consumer_id, msg_id+1, sync=True)
            
            if msg_id not in self.msg_dict:
                return None, msg_id
                
            return self.msg_dict[msg_id], msg_id
        except SyncObjException:
            raise Exception("Unable to get message from the partition due to timeout")
        except Exception as e:
            raise e

    def has_consumer(self, consumer_id):
        if consumer_id in self.consumer_dict:
            return True
        return False
    
    def add_consumer(self, consumer_id):
        # print(self.consumer_dict)
        # if consumer_id in self.consumer_dict:
        #     raise Exception("Consumer already registered")
        try:
            self.consumer_dict.set(consumer_id, 0, sync=True)
        except SyncObjException:
            raise Exception("Unable to add consumer to partition due to timeout")
        except Exception as e:
            raise e
        
    def get_consumer_offset(self, consumer_id):
        if consumer_id not in self.consumer_dict:
            raise Exception(f"Consumer_id={consumer_id} not registered")
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

    def remove(self):
        self.sync_obj.destroy()

    def __str__(self) -> str:
        return f"PartitionRaft-{self.topic_name}-{self.partition_id}"
    
## End of iteration-2 Model


class PartitionDict:
    def __init__(self):
        self.partitions = {}
        self.lock = threading.Lock()

    # def add_partition(self, partition: PartitionRaft):
    #     self.lock.acquire()
    #     self.partitions[(partition.topic_name, partition.partition_id)] = partition
    #     self.lock.release()

    def add_partition_inplace(self, raft_host:str, raft_partners:list, topic_name:str, partition_id:int, replica_id:int):
        self.lock.acquire()
        partition = PartitionRaft(raft_host, raft_partners, topic_name, partition_id)
        self.partitions[(topic_name, partition_id)] = {
            'partition': partition,
            'replica_id': replica_id
        }
        # if using PartitionRaft inherited from Syncobj, pass the object itself
        # partition.waitBinded()
        # partition.waitReady()
        
        # # Otherwise if using batteries, 
        partition.sync_obj.waitBinded()
        partition.sync_obj.waitReady()

        def run_sync_obj(ref):
            sync_obj = ref()
            while True:
                sync_obj.doTick()
                time.sleep(0.1)
                # print(25*"-")
                # print(sync_obj.getStatus())
                # print(25*"-")
                # if(sync_obj.getStatus()['has_quorum'] == True):
                    # print("Done")
                    # break

        from threading import Thread
        thread = Thread(
            target=run_sync_obj, 
            # if using PartitionRaft inherited from Syncobj, pass the object itself
            # args=(weakref.ref(partition),)
            
            # # Otherwise if using batteries, of iteration-2
            args=(weakref.ref(partition.sync_obj),)
        )
        thread.start()

        # No need as killing the parent will kill the threads as well    
        # self.partitions[(topic_name, partition_id)]['thread'] = thread

        self.lock.release()

    def get_partition(self, topic_name:str, partition_id: int):
        if (topic_name, partition_id) not in self.partitions:
            return None
        
        return self.partitions[(topic_name, partition_id)]['partition']

    def get_replica_id(self, topic_name:str, partition_id: int):
        if (topic_name, partition_id) not in self.partitions:
            return None
        return self.partitions[(topic_name, partition_id)]['replica_id']
    
    def get_partitions(self):
        return [d['partition'] for d in self.partitions.values()]

    # No need as killing the parent will kill the threads as well    
    # def get_threads(self):
    #     return [d['thread'] for d in self.partitions.values()]
    
    def get_keys(self):
        return list(self.partitions.keys())
    
partitions = PartitionDict()

def get_partitions():
    global partitions
    return partitions