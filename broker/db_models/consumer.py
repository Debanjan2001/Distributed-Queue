from src.app import db

class ConsumerModel(db.Model):
    __tablename__ = 'consumer'

    consumer_id = db.Column(db.String, primary_key=True)
    topic_name = db.Column(db.String, nullable=False)
    partition_id = db.Column(db.Integer, nullable=False)
    msg_offset = db.Column(db.Integer, nullable=False)