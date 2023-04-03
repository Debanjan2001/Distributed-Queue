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
        parser.add_argument('broker_id', type=str, required=True)
        parser.add_argument('broker_addr', type=str, required=True)
        args = parser.parse_args()

        try:
            add_broker(args['broker_id'], args['broker_addr'])
        except Exception as e:
            return {
                "status": "Failed",
                "reason": str(e)
            }, HTTP_400_BAD_REQUEST
        return {
            "status": "Success",
        }, HTTP_201_CREATED

api.add_resource(HeartbeatAPI, "/")
api.add_resource(RAFTStatusAPI, "/raft_status")
api.add_resource(BrokerAPI, "/brokers")
