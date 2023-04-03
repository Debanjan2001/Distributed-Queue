from argparse import ArgumentParser
from src.app import create_app

def main():
    parser = ArgumentParser()
    parser.add_argument('--flask_host', default='0.0.0.0:5000')
    parser.add_argument('--raft_host', default='0.0.0.0:6000')
    parser.add_argument('--partners', nargs='+')
    parser.add_argument('--debug', action='store_true')

    args = parser.parse_args()
    flask_host = args.flask_host.split(':')[0]
    flask_port = args.flask_host.split(':')[1]
    raft_host = args.raft_host
    partners = args.partners

    app = create_app(raft_host, partners)
    if args.debug:
        app.run(host=flask_host, port=flask_port, debug=True)
    else:
        app.run(host=flask_host, port=flask_port)



if __name__ == '__main__':
    main()
