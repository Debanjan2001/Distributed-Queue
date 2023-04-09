from flask import Blueprint
from flask_restful import (
    Resource, 
    Api, 
    reqparse,
)
from src.http_status_codes import *

from src.raft import (
    partitions,
    # get_partitions
)

from src.app import app, db
from db_models import LogModel, PartitionModel

api_bp = Blueprint('api', __name__)
api = Api(api_bp)
app.register_blueprint(api_bp)

class HeartbeatAPI(Resource):
    def get(self):
        return {
            "status": "Success",
        }, HTTP_200_OK
    

class RAFTStatusAPI(Resource):
    def get(self):
        parser = reqparse.RequestParser()
        parser.add_argument('topic_name', type=str, required=True)
        parser.add_argument('partition_id', type=int, required=True)
        args = parser.parse_args()

        try:
            partition = partitions.get_partition(args.topic_name, args.partition_id)
            if partition is None:
                return {
                    "status": "Failure",
                    "reason": f"No partition {args.partition_id} in topic {args.topic_name}"
                }, HTTP_400_BAD_REQUEST
            
            raft_status = partition.get_raft_status()

            return {
                "status": "Success",
                "raft_status": raft_status
            }, HTTP_200_OK
        except Exception as e:
            return {
                "status": "Failure",
                "reason": str(e)
            }, HTTP_400_BAD_REQUEST
    

class MessageAPI(Resource):
    def get(self):
        parser = reqparse.RequestParser()
        parser.add_argument('topic_name', type=str, required=True)
        parser.add_argument('partition_id', type=int, required=True)
        parser.add_argument('consumer_id', required=True)
        args = parser.parse_args()

        try:
            partition = partitions.get_partition(args.topic_name, args.partition_id)
            if partition is None:
                return {
                    "status": "Failure",
                    "reason": f"No partition {args.partition_id} in topic {args.topic_name}"
                }, HTTP_400_BAD_REQUEST
            
            if not partition.has_consumer(args.consumer_id):
                partition.add_consumer(args.consumer_id)
                
            msg, msg_id = partition.get_message(args.consumer_id)
            
            if msg_id is None:
                return {
                    "status": "Success",
                    "message": "No unread messages found"
                }, HTTP_200_OK
            
            if msg is None:
                msg = LogModel.query.filter_by(topic_name=args.topic_name, partition_id=args.partition_id, msg_offset=msg_id).first().msg

            return {
                "status": "Success",
                "message": msg
            }, HTTP_200_OK
        
        except Exception as e:
            print(e)
            return {
                "status": "Failure",
                "reason": str(e)
            }, HTTP_400_BAD_REQUEST

    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('topic_name', type=str, required=True)
        parser.add_argument('partition_id', type=int, required=True)
        parser.add_argument('message', type=str, required=True)
        args = parser.parse_args()

        try:
            partition = partitions.get_partition(args.topic_name, args.partition_id)
            if partition is None:
                return {
                    "status": "Failure",
                    "reason": f"No partition with ID {args.partition_id}"
                }, HTTP_400_BAD_REQUEST

            msg_id = partition.add_message(args.message)

            # DB updates
            log = LogModel(topic_name = args.topic_name,
                           partition_id = args.partition_id,
                           msg = args.message,
                           msg_offset = msg_id)
            db.session.add(log)
            db.session.commit()

            partition_db_obj = PartitionModel.query.filter_by(topic_name=args.topic_name, partition_id=args.partition_id).first()
            partition_db_obj.msg_count += 1
            db.session.commit()

            partition.unset_msg_replicated_bit(msg_id, args.partition_id)
            
            return {
                "status": "Success",
                "message": f"Message added to Partition_id = {args.partition_id} of `{args.topic_name}`"
            }, HTTP_201_CREATED
        
        except Exception as e:
            return {
                "status": "Failure",
                "reason": str(e)
            }, HTTP_400_BAD_REQUEST

class ConsumerAPI(Resource):
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('topic_name', type=str, required=True)
        parser.add_argument('partition_id',type=int, required=True)
        parser.add_argument('consumer_id', required=True)
        args = parser.parse_args()

        try:
            partition = partitions.get_partition(args.topic_name, args.partition_id)
            if partition is None:
                return {
                    "status": "Failure",
                    "reason": f"No partition with ID {args.partition_id}"
                }, HTTP_400_BAD_REQUEST
            
            if partition.has_consumer(args.consumer_id):
                return {
                    "status": "Failure",
                    "reason": f"Consumer ID {args.consumer_id} already registered"
                }, HTTP_400_BAD_REQUEST
            
            partition.add_consumer(args.consumer_id)

            # DB update
            # consumer = ConsumerModel(consumer_id=args.consumer_id,
            #                          topic_name=args.topic_name,
            #                          partition_id=args.partition_id,
            #                          msg_offset=0)
            # db.session.add(consumer)
            # db.session.commit()

            return {
                "status": "Success",
                "Message": f"Consumer#{args.consumer_id} registered successfully for Topic: {args.topic_name}, Partition: {args.partition_id}"
            }, HTTP_201_CREATED
        
        except Exception as e:
            return {
                "status": "Failure",
                "reason": str(e)
            }, HTTP_400_BAD_REQUEST


class PartitionAPI(Resource):
    # Add partition to broker
    def get(self):
        all_partitions = partitions.get_keys()
        return {
            "status": "Success",
            "partitions": all_partitions
        }, HTTP_200_OK
        
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('topic_name',type=str, required=True)
        parser.add_argument('partition_id',type=int, required=True)
        parser.add_argument('replica_id',type=int, required=True)
        parser.add_argument('raft_host',type=str, required=True)
        parser.add_argument('raft_partners', type=str, action="append")
        args = parser.parse_args()

        try:
            if ('raft_partners' not in args) or (not args['raft_partners']):
                args['raft_partners'] = None

            if partitions.get_partition(args.topic_name, args.partition_id) is not None:
                return {
                    "status": "Failure",
                    "reason": f"Partition {args.partition_id} already exists of topic {args.topic_name}"
                }, HTTP_400_BAD_REQUEST

            partitions.add_partition_inplace(
                raft_host=args.raft_host,
                raft_partners=args.raft_partners,
                topic_name=args.topic_name,
                partition_id=args.partition_id,
                replica_id=args.replica_id,
            )

            # DB Update
            partition = PartitionModel(topic_name=args["topic_name"],
                                       partition_id=args["partition_id"],
                                       msg_count=0,
                                       replica_id=args["replica_id"],
                                       raft_host=args["raft_host"],
                                       raft_partners=str(args["raft_partners"]))
            db.session.add(partition)
            db.session.commit()

            return {
                "status": "Success",
            }, HTTP_201_CREATED
        
        except Exception as e:
            return {
                "status": "Failure",
                "reason": str(e)
            }, HTTP_400_BAD_REQUEST


api.add_resource(HeartbeatAPI, "/")
api.add_resource(RAFTStatusAPI, "/raft_status")
api.add_resource(MessageAPI, "/logs")
api.add_resource(PartitionAPI, "/partitions")
api.add_resource(ConsumerAPI, "/consumers")