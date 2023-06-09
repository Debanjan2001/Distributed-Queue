from argparse import ArgumentParser
from src.app import create_app
import os

def main():
    parser = ArgumentParser()
    parser.add_argument('--flask_host', default='0.0.0.0:5000')
    parser.add_argument('--raft_host', default='0.0.0.0:6000')
    parser.add_argument('--partners', nargs='+')

    args = parser.parse_args()
    flask_host = args.flask_host.rsplit(':', 1)[0]
    flask_port = args.flask_host.rsplit(':', 1)[1]
    raft_host = args.raft_host
    partners = args.partners

    os.makedirs(f'{os.getcwd()}/raft_logs/manager/', exist_ok=True)

    app = create_app(raft_host, partners)
    app.run(host=flask_host, port=flask_port)



if __name__ == '__main__':
    main()
