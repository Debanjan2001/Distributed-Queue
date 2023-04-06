from flask import Blueprint
from src.app import create_app
from flask_restful import (
    Resource, 
    Api, 
    reqparse,
)
from src.http_status_codes import *
from src.raft import (
    # get_partitions,
    PartitionRaft,
    PartitionDict,
    # check_status,
)



api_bp = Blueprint('api', __name__)
api = Api(api_bp)

import threading

partitions = PartitionDict()
# partition_list = []
partition = None
# partitions = get_partitions() 

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

        # global partitions, partition_list
        partition = partitions.get_partition(args.topic_name, args.partition_id)
        # global partition
        if partition is None:
            return {
                "status": "Failure",
                "reason": f"No partition {args.partition_id} in topic {args.topic_name}"
            }, HTTP_400_BAD_REQUEST
        raft_status = partition.get_raft_status()

        # if len(partition_list) is 0:
        #     return {
        #         "status": "Failure",
        #         "reason": f"No partition {args.partition_id} in topic {args.topic_name}"
        #     }, HTTP_400_BAD_REQUEST
        # partition = partition_list[0]
        # flag = False
        # for _ in range(10):
        #     raft_status = partition.get_raft_status()
        #     if(raft_status.get("has_quorum") is True):
        #         flag = True
        #         break
        # print("FLAG ==",flag)
        return {
            "status": "Success",
            "raft_status": raft_status
        }, HTTP_200_OK
    

class MessageAPI(Resource):
    def get(self):
        parser = reqparse.RequestParser()
        parser.add_argument('topic_name', type=str, required=True)
        parser.add_argument('partition_id', type=int, required=True)
        parser.add_argument('consumer_id', required=True)
        args = parser.parse_args()

        # global partitions
        partition = partitions.get_partition(args.topic_name, args.partition_id)
        if partition is None:
            return {
                "status": "Failure",
                "reason": f"No partition {args.partition_id} in topic {args.topic_name}"
            }, HTTP_400_BAD_REQUEST
        
        msg = partition.get_message(args.consumer_id)
        if msg is None:
            return {
                "status": "Failure",
                "reason": f"No message for consumer ID {args.consumer_id} in partition {args.partition_id}"
            }, HTTP_400_BAD_REQUEST
        
        return {
            "status": "Success",
            "message": msg
        }, HTTP_200_OK

    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('topic_name', type=str, required=True)
        parser.add_argument('partition_id', type=int, required=True)
        parser.add_argument('msg', type=str, required=True)
        args = parser.parse_args()
        
        # global partitions
        partition = partitions.get(args.topic_name, args.partition_id)
        if partition is None:
            return {
                "status": "Failure",
                "reason": f"No partition with ID {args.partition_id}"
            }, HTTP_400_BAD_REQUEST

        partition.add_message(args.msg)
        return {
            "status": "Success",
            "message": f"Message added to Partition_id = {args.partition_id} of `{args.topic_name}`"
        }, HTTP_201_CREATED

class ConsumerAPI(Resource):
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('topic_name', type=str, required=True)
        parser.add_argument('partition_id',type=int, required=True)
        parser.add_argument('consumer_id', required=True)
        args = parser.parse_args()

        
        # global partitions
        partition = partitions.get_partition(args.topic_name, args.partition_id)
        if partition is None:
            return {
                "status": "Failure",
                "reason": f"No partition with ID {args.partition_id}"
            }, HTTP_400_BAD_REQUEST
        
        try:
            partition.add_consumer(args.consumer_id)
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
        
        # global partitions
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

        if ('raft_partners' not in args) or (not args['raft_partners']):
            args['raft_partners'] = None

        # global partitions, partition_list
        if partitions.get_partition(args.topic_name, args.partition_id) is not None:
            return {
                "status": "Failure",
                "reason": f"Partition {args.partition_id} already exists of topic {args.topic_name}"
            }, HTTP_400_BAD_REQUEST

        # print(args)
        
        # partition = PartitionRaft(
        #     raft_host=args.raft_host, 
        #     raft_partners=args.raft_partners, 
        #     topic_name=args.topic_name,
        #     partition_id=args.partition_id,
        #     replica_id=args.replica_id,
        # )

        # partition.waitBinded()
        # partition.waitReady()

        # while(True):
        #     import time
        #     time.sleep(0.5)
        #     print(partition.getStatus())
        #     print(25*"-")

        # import time
        # time.sleep(0.5)

        # partitions.add_partition(partition)
        # partition_list.append(partition)

        partitions.add_partition_inplace(
            raft_host=args.raft_host,
            raft_partners=args.raft_partners,
            topic_name=args.topic_name,
            partition_id=args.partition_id,
            replica_id=args.replica_id,
        )

        return {
            "status": "Success",
        }, HTTP_201_CREATED


api.add_resource(HeartbeatAPI, "/")
api.add_resource(RAFTStatusAPI, "/raft_status")
api.add_resource(MessageAPI, "/logs")
api.add_resource(PartitionAPI, "/partitions")
api.add_resource(ConsumerAPI, "/consumers")