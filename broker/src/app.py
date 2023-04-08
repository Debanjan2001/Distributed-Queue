import logging
import signal
from typing import List
from flask import Flask
import threading
from src.raft import (
    PartitionDict
)

from src.views import (
    get_partitions
)
def create_app():
    # for development purposes only
    logging.basicConfig(level=logging.DEBUG, force=True)

    app = Flask(__name__)

    def sigint_handler(signum, frame):
        partitions:PartitionDict = get_partitions()

        for partition_raft in partitions.get_partitions():
            print(f"Destroying {partition_raft}")
            partition_raft.remove()
        
        exit(0)

    signal.signal(signal.SIGINT, sigint_handler)
    return app