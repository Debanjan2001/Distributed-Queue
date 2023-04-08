from src.raft import get_partitions
from src.app import db, app
from db_models import LogModel
from time import sleep
from threading import Thread
import signal
import sys

def run_periodic_db_dump():
    with app.app_context():
        partitions = get_partitions()
        while(True):
            print("Db dump thread running...")
            for partition_raft in partitions.get_partitions():
                for msg_id in list(partition_raft.msg_dict.keys()):
                    msg_object = partition_raft.msg_dict[msg_id]
                    bitmask = msg_object[1]
                    replica_id = partitions.get_replica_id(
                        topic_name=partition_raft.topic_name,
                        partition_id=partition_raft.partition_id
                    )
                    if ((bitmask >> replica_id) & 1)==0:
                        continue
                    msg = msg_object[0]
                    try:
                        if LogModel.query.filter_by(
                            topic_name=partition_raft.topic_name,
                            partition_id=partition_raft.partition_id,
                            msg_offset=msg_id
                        ).first() is not None:
                            partition_raft.unset_msg_replicated_bit(msg_id, replica_id)
                            raise Exception("DB_DUMP_THREAD:: Message already exists in DB")
                        
                        log = LogModel(
                            topic_name=partition_raft.topic_name,
                            partition_id=partition_raft.partition_id,
                            msg_offset=msg_id,
                            msg=msg,
                            msg_replicated_bitmask=bitmask
                        )
                        db.session.add(log)
                        db.session.commit()
                        partition_raft.unset_msg_replicated_bit(msg_id, replica_id)

                    except Exception as e:
                        print(str(e))
                        
            sleep(30)

def run_msg_handler_thread():
    t = Thread(target=run_periodic_db_dump, daemon=True)
    t.start()

    def sigint_handler(signum, frame):
        print("Db dump thread exiting...")
        exit(0)

    signal.signal(signal.SIGINT, sigint_handler)