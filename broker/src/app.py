import logging
import signal
from typing import List
from flask import Flask
import threading
from src.raft import (
    PartitionDict
)

def create_app():
    # for development purposes only
    logging.basicConfig(level=logging.DEBUG, force=True)

    app = Flask(__name__)

    def sigint_handler(signum, frame):
        # partitions = get_partitions()
        # for partition_raft in partitions.get_partitions():
        #     print("Destroying")
        #     partition_raft.destroy()
        exit(0)

    signal.signal(signal.SIGINT, sigint_handler)
    return app