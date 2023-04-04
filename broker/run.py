from flask import Flask
from argparse import ArgumentParser
from src import views
import subprocess

app = Flask(__name__)
partition_dict = {}

class GlobalDict():
    def add_partition(partition_id, sync_obj, data_list):
        partition_dict[partition_id] = [sync_obj, data_list]

    def inc_msg_cnt():
        pass

    def add_data_dict():
        pass

    def inc_consumer_dict():
        pass

    def get_data(partition_id):
        if partition_id not in partition_dict:
            raise Exception("Partition not present in broker")
        
        obj = partition_dict[partition_id]
        return obj[1]

def main():
    parser = ArgumentParser()
    parser.add_argument('--flask_host', default='0.0.0.0:5000')
    parser.add_argument('--debug', action='store_true')
    args = parser.parse_args()

    flask_host = args.flask_host.split(':')[0]
    flask_port = args.flask_host.split(':')[1]

    i = 5000
    while subprocess.run("lsof -i:" + str(i), capture_output=True, shell=True).stdout.decode() != "":
        i = i + 1
    flask_port = i

    app.register_blueprint(views.api_bp)
    if args.debug:
        app.run(host=flask_host, port=flask_port, debug=True)
    else:
        app.run(host=flask_host, port=flask_port)



if __name__ == '__main__':
    main()