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
from transport_stream.stream_utils import subscribe_and_tag
from transport_stream.stream_utils import encode_elementary_stream_message
from transport_stream.stream_utils import decode_elementary_stream_message
from transport_stream.stream_utils import fan_out_tags


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

        def verbose(m):
            print(m)
            return m

        verbose = lambda x: x

        while True:
            publish_to_kinesis(
                client=kinesis_client,
                streamname=config['stream_name'],
                message_stream=map(
                    verbose,
                    map(
                        encode_elementary_stream_message,
                        subscribe_and_tag(
                            context=context,
                            tagged_addresses=config['elementary_streams']))))

    elif config["mode"] == 'demux':
        fan_out = fan_out_tags(
            context, tagged_addresses=config['elementary_streams'])
        fan_out.send(None)

        while True:
            for record in yield_kinesis_records(
                    stream_name=config['stream_name'], client=kinesis_client):
                if record:
                    fan_out.send(json.loads(record))

    else:
        raise Exception("unrecognized mode")
