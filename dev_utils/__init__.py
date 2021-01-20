import time
import argparse
import zlib
import yaml
from itertools import repeat
import json
from random import uniform
import sys

import zmq
import boto3
import botocore.config
from botocore.exceptions import ClientError
import argcomplete

from dotenv import load_dotenv
load_dotenv()


def dev_publisher():
    """
    """

    context = zmq.Context()

    parser = argparse.ArgumentParser(usage=None)
    parser.add_argument("-p", "--pub-port", type=str, required=True)
    parser.add_argument("-w", "--wait", action='store_true', default=False)
    args = parser.parse_args()
    argcomplete.autocomplete(parser)
    args_vars = vars(args)

    context = zmq.Context()
    socket = context.socket(zmq.PUB)

    socket.bind(f'tcp://127.0.0.1:{args.pub_port}')

    for val in enumerate(repeat('x')):
        time.sleep(uniform(0.0, 0.6))
        print(f"<{args.pub_port}> sending: {json.dumps(val).encode()}")
        socket.send(json.dumps(val).encode())


def dev_scope():

    context = zmq.Context()

    parser = argparse.ArgumentParser(usage=None)
    parser.add_argument("sub-port", type=str)
    args = parser.parse_args()
    argcomplete.autocomplete(parser)
    args_vars = vars(args)

    socket = context.socket(zmq.SUB)
    socket.setsockopt_string(zmq.SUBSCRIBE, "")
    socket.setsockopt(zmq.RCVHWM, 30000)

    socket.connect(args.sub_port)

    while True:
        raw_message = socket.recv().decode()
        try:
            sys.stdout.write(raw_message)
            sys.stdout.write("\n")
            sys.stdout.flush()
        except ValueError:
            print(raw_message)
