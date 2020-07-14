import argparse
import zlib
import yaml

import zmq
import boto3
import botocore.config
from botocore.exceptions import ClientError


def dev_publisher():
    """
    """

    context = zmq.Context()

    parser = argparse.ArgumentParser(usage=None)
    parser.add_argument("--pub-address", type=str, required=True)
    parser.add_argument("-w", "--wait", action='store_true', default=False)
    args = parser.parse_args()
    argcomplete.autocomplete(parser)
    args_vars = vars(args)

    context = zmq.Context()
    socket = context.socket(zmq.PUB)

    socket.bind('tcp://127.0.0.1:5555')

    while True:
        val = input()
        socket.send(val.encode())


def run_service():
    """
    """

    from os import environ

    # parser = argparse.ArgumentParser(usage=None)
    # parser.add_argument("--sub-address", type=str, required=True)
    # parser.add_argument(
    #     "--region", type=str, default="us-east-1", help=("region"))
    # parser.add_argument(
    #     "--profile",
    #     type=str,
    #     #default="flight-data-service-client",
    #     help=("profile to use"))
    # args = parser.parse_args()
    # args_vars = vars(args)

    # General Config
    AWS_DEFAULT_REGION = environ.get('AWS_DEFAULT_REGION')
    AWS_DEFAULT_PROFILE = environ.get('AWS_DEFAULT_PROFILE')

    SERVICE_CONFIG_FILE = environ.get('TS_CONFIG_FILE')

    context = zmq.Context()
    socket = context.socket(zmq.SUB)

    socket.setsockopt_string(zmq.SUBSCRIBE, 'tcp://127.0.0.1:5555')

    print(AWS_DEFAULT_REGION, AWS_DEFAULT_PROFILE, SERVICE_CONFIG_FILE)
    while True:
        print(socket.recv().decode())
