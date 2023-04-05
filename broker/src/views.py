from flask import Blueprint
from run import GlobalDict
from src.app import create_app
from flask_restful import (
    Resource, 
    Api, 
    reqparse,
)
from src.http_status_codes import *
from src.raft import (
    get_raft_status,
    get_data_dict,
    get_consumer_dict,
    add_broker,
)
from src import (
    app
)

api_bp = Blueprint('api', __name__)
api = Api(api_bp)

class HeartbeatAPI(Resource):
    def get(self):
        return {
            "status": "Success",
        }, HTTP_200_OK
    
# class RAFTStatusAPI(Resource):
#     def get(self):
#         return {
#             "status": "Success",
#             "raft_status": get_raft_status()
#         }, HTTP_200_OK
    

class MessageAPI(Resource):
    def get(self):
        parser = reqparse.RequestParser()
        parser.add_argument('topic_name', type=str, required=True)
        parser.add_argument('partition_id', type=int, required=True)
        parser.add_argument('consumer_id', type=int, required=True)
        args = parser.parse_args()

        data_list = GlobalDict.get_data(args.partition_id)
        msg_cnt = data_list[0]
        data_dict = data_list[1]
        consumer_dict = data_list[2]

        if args.consumer_id not in consumer_dict:
            return {
                "status": "Failure",
                "reason": f"No message for consumer ID {args.consumer_id} in partition {args.partition_id}"
            }, HTTP_400_BAD_REQUEST
        
        offset = consumer_dict[args.consumer_id]
        if offset > msg_cnt:
            return {
                "status": "Failure",
                "reason": f"No messages left for consumption in topic {args.topic_name}"
            }, HTTP_400_BAD_REQUEST
        
        return {
            "status": "Success",
            "data": data_dict[offset]
        }, HTTP_200_OK
    
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('topic_name', type=str, required=True)
        parser.add_argument('partition_id', type=int, required=True)
        parser.add_argument('msg', type=str, required=True)
        args = parser.parse_args()

        data_list = GlobalDict.get_data(args.partition_id)
        msg_cnt = data_list[0]
        data_dict = data_list[1]
        consumer_dict = data_list[2]

        try:
            msg_cnt.inc(sync=False)
            offset = consumer_dict[args.consumer_id] + 1
            consumer_dict[args.consumer_id] = offset
            data_dict[offset] = [args.msg, 7]
        except Exception as e:
            return {
                "status": "Failed",
                "reason": str(e)
            }, HTTP_400_BAD_REQUEST
        
        return {
            "status": "Success",
        }, HTTP_201_CREATED
    
class PartitionAPI(Resource):
    # Add partition to broker
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('topic_name', required=True)
        parser.add_argument('partition_id', required=True)
        parser.add_argument('raft_host', required=True)
        parser.add_argument('raft_partners', required=True, multiple=True)
        args = parser.parse_args()

        sync_obj, data_list = create_app(args.raft_host, args.partners)
        GlobalDict.add_partition(args.partition_id, sync_obj, data_list)

        return {
            "status": "Success",
        }, HTTP_201_CREATED

# class AddBroker(Resource):
#     def post(self):
#         parser = reqparse.RequestParser()
#         parser.add_argument('broker_ip', type=str, required=True)
#         parser.add_argument('broker_port', type=str, required=True)
#         args = parser.parse_args()

#         return {
#             "status": "Success",
#         }, HTTP_201_CREATED

api.add_resource(HeartbeatAPI, "/")
# api.add_resource(RAFTStatusAPI, "/raft_status")
api.add_resource(MessageAPI, "/logs")
api.add_resource(PartitionAPI, "/partitions")