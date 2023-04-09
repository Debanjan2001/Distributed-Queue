from flask import Blueprint
from flask_restful import (
    Resource, 
    Api, 
    reqparse,
)
from src.http_status_codes import *
from src.raft import (
    get_raft_status,
    get_broker_dict,
    add_broker,
    add_topic,
    add_producer,
    add_consumer,
    add_partition,
    is_producer,
    is_consumer,
    get_brokers_for_topic_partition
)
from src import (
    app
)
import requests

api_bp = Blueprint('api', __name__)
api = Api(api_bp)

class HeartbeatAPI(Resource):
    def get(self):
        return {
            "status": "Success",
        }, HTTP_200_OK
    
class RAFTStatusAPI(Resource):
    def get(self):
        return {
            "status": "Success",
            "raft_status": get_raft_status()
        }, HTTP_200_OK
    

class BrokerAPI(Resource):
    def get(self):
        broker_dict = get_broker_dict()
        return {
            "status": "Success",
            "brokers": dict(broker_dict.items())
        }, HTTP_200_OK
    
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('broker_addr', type=str, required=True)
        args = parser.parse_args()

        try:
            add_broker(args['broker_addr'])
        except Exception as e:
            return {
                "status": "Failed",
                "reason": str(e)
            }, HTTP_400_BAD_REQUEST
        return {
            "status": "Success",
        }, HTTP_201_CREATED

class TopicAPI(Resource):
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('topic_name', type=str, required=True)
        args = parser.parse_args()

        try:
            add_topic(args['topic_name'])
        except Exception as e:
            return {
                "status": "Failed",
                "reason": str(e)
            }, HTTP_400_BAD_REQUEST
        return {
            "status": "Success",
        }, HTTP_201_CREATED
    
class PartitionAPI(Resource):
    def get(self):
        parser = reqparse.RequestParser()
        parser.add_argument('topic_name', type=str, required=True)
        parser.add_argument('partition_id', type=int, required=True)
        args = parser.parse_args()

        broker_list = get_brokers_for_topic_partition(args["topic_name"], args["partition_id"])
        return {
            "status": "Success",
            "brokers": broker_list
        }, HTTP_200_OK
    
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('producer_id', type=str, required=True)
        parser.add_argument('topic_name', type=str, required=True)
        parser.add_argument('partition_id', type=int, required=True)
        args = parser.parse_args()

        if not is_producer(args["producer_id"], args["topic_name"]):
            return {
                "status": "Failed",
                "reason": f"Producer with id({args['producer_id']}) doesn't exist for topic {args['topic_name']}"
            }, HTTP_400_BAD_REQUEST
        
        try:
            add_partition(args["topic_name"])
        except Exception as e:
            return {
                "status": "Failed",
                "reason": str(e)
            }, HTTP_400_BAD_REQUEST
        
        return {
            "status": "Success",
        }, HTTP_201_CREATED
    
class ProducerAPI(Resource):
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('topic_name', type=str, required=True)
        args = parser.parse_args()

        try:
            producer_id = add_producer(args['topic_name'])
        except Exception as e:
            return {
                "status": "Failed",
                "reason": str(e)
            }, HTTP_400_BAD_REQUEST
        return {
            "status": "Success",
            "producer_id": producer_id 
        }, HTTP_201_CREATED
    
class ConsumerAPI(Resource):
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('topic_name', type=str, required=True)
        args = parser.parse_args()

        try:
            consumer_id = add_consumer(args['topic_name'])
        except Exception as e:
            return {
                "status": "Failed",
                "reason": str(e)
            }, HTTP_400_BAD_REQUEST
        return {
            "status": "Success",
            "consumer_id": consumer_id 
        }, HTTP_201_CREATED
    
class MessageAPI(Resource):
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('producer_id', type=str, required=True)
        parser.add_argument('topic_name', type=str, required=True)
        parser.add_argument('partition_id', type=int, required=True)
        parser.add_argument('message', type=str, required=True)
        args = parser.parse_args()

        if not is_producer(args["producer_id"], args["topic_name"]):
            return {
                "status": "Failed",
                "reason": f"Producer with id({args['producer_id']}) doesn't exist for topic {args['topic_name']}"
            }, HTTP_400_BAD_REQUEST
        
        brokers = get_brokers_for_topic_partition(args["topic_name"], args["partition_id"])
        if brokers is None:
            return {
                "status": "Failed",
                "reason": f"Topic or partition doesn't exist"
            }, HTTP_400_BAD_REQUEST
        
        for broker in brokers:
            response = requests.post(
                url = broker + "/logs", 
                json = {
                    "topic_name": args["topic_name"],
                    "partition_id": int(args["partition_id"]),
                    "message": args["message"]
                }                   
            )
            if response.status_code == HTTP_201_CREATED:
                return {
                "status": "Success",
                "reason": f"Message successfuly posted"
            }, HTTP_201_CREATED

        return {
                "status": "Failed",
                "reason": f"Unable to post message"
            }, HTTP_400_BAD_REQUEST
    
    def get(self):
        parser = reqparse.RequestParser()
        parser.add_argument('consumer_id', type=str, required=True)
        parser.add_argument('topic_name', type=str, required=True)
        parser.add_argument('partition_id', type=int, required=True)
        args = parser.parse_args()

        if not is_consumer(args["consumer_id"], args["topic_name"]):
            return {
                "status": "Failed",
                "reason": f"Consumer with id({args['consumer_id']}) doesn't exist for topic {args['topic_name']}"
            }, HTTP_400_BAD_REQUEST
        
        brokers = get_brokers_for_topic_partition(args["topic_name"], args["partition_id"])
        if brokers is None:
            return {
                "status": "Failed",
                "reason": f"Topic or partition doesn't exist"
            }, HTTP_400_BAD_REQUEST
        
        for broker in brokers:
            response = requests.get(
                url = broker + "/logs", 
                json = {
                    "topic_name": args["topic_name"],
                    "partition_id": int(args["partition_id"]),
                    "consumer_id": args["consumer_id"]
                }                   
            )

            if response.status_code == HTTP_200_OK:
                return {
                    "status": "Success",
                    "message": response.json().get("message")
                }, HTTP_200_OK

        return {
                "status": "Failed",
                "reason": f"Unable to fetch message."
            }, HTTP_400_BAD_REQUEST
    
api.add_resource(HeartbeatAPI, "/")
api.add_resource(RAFTStatusAPI, "/raft_status")
api.add_resource(BrokerAPI, "/brokers")
api.add_resource(TopicAPI, "/topics")
api.add_resource(PartitionAPI, "/partitions")
api.add_resource(ProducerAPI, "/producers")
api.add_resource(ConsumerAPI, "/consumers")
api.add_resource(MessageAPI, "/messages")