from flask import Flask
from argparse import ArgumentParser
from src.app import create_app
from src import views
import subprocess
import threading

from src.raft import PartitionRaft, PartitionDict

# global s
# s = None

def run_thread(port):

    from pysyncobj import SyncObj
    if int(port[-1]) % 2 == 0:
        s = SyncObj("localhost:20010", ["localhost:20011"])
    elif int(port[-1]) % 2 == 1:
        s = SyncObj("localhost:20011", ["localhost:20010"])
    s.waitBinded()
    s.waitReady()

    import time
    while(True):
        time.sleep(3)
        print(s.getStatus())



def main():
    parser = ArgumentParser()
    parser.add_argument('--flask_host', default='0.0.0.0:8000')
    args = parser.parse_args()

    flask_host = args.flask_host.rsplit(':', 1)[0]
    flask_port = args.flask_host.rsplit(':', 1)[1]

    # i = 8000
    # while subprocess.run("lsof -i:" + str(i), capture_output=True, shell=True).stdout.decode() != "":
    #     i = i + 1
    # flask_port = i

    # t = threading.Thread(target=run_thread, args=(flask_port,))
    # t.start()

    # from pysyncobj import SyncObj
    # port=flask_port
    # if int(port[-1]) % 2 == 0:
    #     # s = SyncObj("localhost:20010", ["localhost:20011"])
    #     s = PartitionRaft("localhost:20010", ["localhost:20011"], "test", 0, 0)
    # elif int(port[-1]) % 2 == 1:
    #     # s = SyncObj("localhost:20011", ["localhost:20010"])
    #     s = PartitionRaft("localhost:20011", ["localhost:20010"], "test", 0, 0)
    # s.waitBinded()
    # s.waitReady()
    # import time
    # time.sleep(3)
    # print(s.getStatus())

    app = create_app()
    app.register_blueprint(views.api_bp)
    app.run(host=flask_host, port=flask_port)


if __name__ == '__main__':
    main()