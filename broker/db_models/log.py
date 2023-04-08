from src.app import db

class LogModel(db.Model):
    __tablename__ = 'log'

    topic_name = db.Column(db.String, nullable=False)
    partition_id = db.Column(db.Integer, nullable=False)
    msg_offset = db.Column(db.Integer, nullable=False)
    msg = db.Column(db.String, nullable=False)

    __table_args__ = (
        db.UniqueConstraint("topic_name", "partition_id", name="topic_partition_constraint"),
    )