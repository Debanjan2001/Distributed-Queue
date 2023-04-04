import logging
import signal
from typing import List
from flask import Flask
import threading
from src.raft import (
    create_sync_obj, 
    get_sync_obj,
    get_distributed_objs
)

def create_app(raft_host: str, partners: List[str]):
    # for development purposes only
    logging.basicConfig(level=logging.DEBUG, force=True)

    create_sync_obj(raft_host, partners)
    sync_obj = get_sync_obj()
    data_list = get_distributed_objs()

    def sigint_handler(signum, frame):
        sync_obj.destroy()
        exit(0)

    signal.signal(signal.SIGINT, sigint_handler)
    return sync_obj, data_list
