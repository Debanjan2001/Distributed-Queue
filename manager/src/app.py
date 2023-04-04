import logging
import signal
from typing import List
from flask import Flask
import threading
from src.raft import (
    create_sync_obj, 
    get_sync_obj
)

from src import views

app = Flask(__name__)

# This needs to imported, otherwise the api endpoints from views aren't integrated. 
from src import views

def create_app(raft_host: str, partners: List[str]):
    # for development purposes only
    logging.basicConfig(level=logging.DEBUG, force=True)
    app = Flask(__name__)

    create_sync_obj(raft_host, partners)
    sync_obj = get_sync_obj()
    app.register_blueprint(views.api_bp)

    def sigint_handler(signum, frame):
        sync_obj.destroy()
        exit(0)

    signal.signal(signal.SIGINT, sigint_handler)
    return app