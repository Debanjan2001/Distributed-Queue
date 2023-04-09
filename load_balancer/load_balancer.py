from flask import Flask, request
import requests
import time
from flask_restful import (
    Resource, 
    Api, 
)
from threading import Thread


app = Flask(__name__)
api = Api(app)

def check_leader(managers):
    while(True):
        for index, manager in enumerate(managers['urls']):
            try:
                response = requests.get(
                    url= manager + "/raft_status",
                )
                # print(manager, response.json()["raft_status"]["is_leader"])
                if response.json()["raft_status"]["is_leader"]:
                    managers['leader_idx'] = index
                    break 
            except:
                managers['leader_idx'] = None

        time.sleep(2)

with app.app_context():
    managers = {
        'leader_idx': None,
        'urls':[
            "http://127.0.0.1:5000",
            "http://127.0.0.1:5001",
            "http://127.0.0.1:5002",
        ]
    }
    t = Thread(target=check_leader, args=(managers,), daemon=True)
    t.start()

class LoadBalancer(Resource):
    def get(self, path=''):
        response = None
        try:
            assert (managers['leader_idx'] is not None)
            leader_manager = managers['urls'][managers['leader_idx']]
            try:
                data = request.json
                response = requests.get(
                    url=leader_manager + "/" + path,
                    json=data
                )
            except:
                response = requests.get(
                    url=leader_manager + "/" + path,
                )
            return response.json(), response.status_code
        except:
            return {
                "status": "Failed",
                "reason": "Manager not Responding"
            }, 400

    def post(self, path=''):
        try:
            assert (managers['leader_idx'] is not None)
            leader_manager = managers['urls'][managers['leader_idx']]
            data=request.json
            response = requests.post(
                url=leader_manager + "/" +path,
                json=data
            )
            return response.json(), response.status_code
        except:
            return {
                "status": "Failed",
                "reason": "Manager not Responding"
            }, 400

api.add_resource(LoadBalancer, '/', '/<path:path>')

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=4000, debug=False)