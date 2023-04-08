from flask import Flask
from argparse import ArgumentParser

from src.app import (
    app, db
)

import os

def main():
    parser = ArgumentParser()
    parser.add_argument('--flask_host', default='0.0.0.0:8000')
    parser.add_argument('--clear_db', action='store_true')
    args = parser.parse_args()

    flask_host = args.flask_host.rsplit(':', 1)[0]
    flask_port = args.flask_host.rsplit(':', 1)[1]

    # app = create_app()

    with app.app_context():
        if args.clear_db:
            print("Clearing Database")
            db.drop_all()

        db.create_all()
        
    app.run(host=flask_host, port=flask_port)


if __name__ == '__main__':
    main()