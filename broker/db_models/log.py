# from src.raft import MSG_ALL_REPLICA_SET_BIT
from src.app import db
MSG_ALL_REPLICA_SET_BIT= (1<<3) - 1

class LogModel(db.Model):
    __tablename__ = 'log'

    topic_name = db.Column(db.String, nullable=False, primary_key=True)
    partition_id = db.Column(db.Integer, nullable=False, primary_key=True)
    msg_offset = db.Column(db.Integer, nullable=False, primary_key=True)
    msg = db.Column(db.String, nullable=False)
    msg_replicated_bitmask = db.Column(db.Integer, default=MSG_ALL_REPLICA_SET_BIT)

    __table_args__ = (
        db.UniqueConstraint("topic_name", "partition_id", "msg_offset", name="log_topic_partition_constraint"),
    )