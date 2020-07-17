import time
import argparse
import zlib
import yaml
from itertools import repeat
import json
from os import environ

import zmq
import boto3
import botocore.config
from botocore.exceptions import ClientError
import argcomplete

#from kinesis.producer import KinesisProducer

from dotenv import load_dotenv
load_dotenv()

from transport_stream.stream_utils import get_kinesis_client
from transport_stream.stream_utils import publish_to_kinesis
from transport_stream.stream_utils import yield_kinesis_records


def run_service():
    """
    """

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

    SERVICE_CONFIG_FILE = environ.get('TS_SERVICE_CONFIG_FILE')

    with open(SERVICE_CONFIG_FILE, 'r') as f:
        config = yaml.safe_load(f)

    context = zmq.Context()

    print(AWS_DEFAULT_REGION, AWS_DEFAULT_PROFILE, SERVICE_CONFIG_FILE)
    print(config)

    kinesis_client = get_kinesis_client(AWS_DEFAULT_PROFILE,
                                        AWS_DEFAULT_REGION)

    if config["mode"] == 'mux':
        socket = context.socket(zmq.SUB)
        socket.setsockopt_string(zmq.SUBSCRIBE, "")

        print(kinesis_client)
        print("listening...")
        socket.connect('tcp://127.0.0.1:5555')

        def yield_from_zmq(socket):
            while True:
                val = socket.recv()
                print(val)
                yield socket.recv()

        while True:
            #val = socket.recv().decode()
            #print(f"received: {val}")
            publish_to_kinesis(
                client=kinesis_client,
                streamname=config['stream_name'],
                message_stream=yield_from_zmq(socket))

    elif config["mode"] == 'demux':
        while True:
            for record in yield_kinesis_records(
                    stream_name=config['stream_name'], client=kinesis_client):
                if record:
                    print(json.loads(record))

    else:
        raise Exception("unrecognized mode")
