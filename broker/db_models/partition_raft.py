from src.app import db

class PartitionModel(db.Model):
    __tablename__ = 'partition'

    topic_name = db.Column(db.String, nullable=False, primary_key=True)
    partition_id = db.Column(db.Integer, nullable=False, primary_key=True)
    msg_count = db.Column(db.Integer, nullable=False)
    replica_id = db.Column(db.Integer, nullable=False)
    raft_host = db.Column(db.String, nullable=False)
    raft_partners = db.Column(db.String, nullable=False)

    __table_args__ = (
        db.UniqueConstraint("topic_name", "partition_id", name="partition_topic_partition_constraint"),
    )