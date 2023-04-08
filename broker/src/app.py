import logging
import signal
from typing import List
from flask import Flask
from src.raft import get_partitions
from flask_sqlalchemy import SQLAlchemy
import os

def create_app():
    # for development purposes only
    logging.basicConfig(level=logging.DEBUG, force=True)

    app = Flask(__name__)
    # Declare db using SQL Alchemy, give the db address
    app.config['SQLALCHEMY_DATABASE_URI'] = f"postgresql://postgres:admin@localhost:5432/{os.getenv('DATABASE_NAME')}"
    db = SQLAlchemy(app)

    def sigint_handler(signum, frame):
        partitions = get_partitions()

        for partition_raft in partitions.get_partitions():
            print(f"Destroying {partition_raft}")
            partition_raft.remove()
        
        exit(0)

    signal.signal(signal.SIGINT, sigint_handler)
    return app, db

app, db = create_app()

from src import views

# from src.utils import init_from_db

# with app.app_context():
#     init_from_db()