from src.raft import partitions
from db_models import PartitionModel, LogModel

def init_from_db():
    all_db_partitions = PartitionModel.query.all()
    for p in all_db_partitions:
        raft_partners = [
            s.strip(" ").strip("\'").strip("\'") 
            for s in p.raft_partners[1:-1].split(',')
        ]
        partitions.add_partition_inplace(
            raft_host=p.raft_host,
            raft_partners=raft_partners,
            topic_name=p.topic_name,
            partition_id=p.partition_id,
            replica_id=p.replica_id
        )

    # all_db_logs = LogModel.query.filter(
    #     LogModel.msg_replicated_bitmask != 0
    # ).order_by(
    #     LogModel.msg_offset.asc()
    # ).all()

    # for log in all_db_logs:
    #     p = partitions.get_partition(log.topic_name, log.partition_id)
    #     p.add_message(log.msg, log.msg_replicated_bitmask)