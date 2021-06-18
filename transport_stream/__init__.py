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

    # General Config
    AWS_DEFAULT_REGION = environ.get('AWS_DEFAULT_REGION')
    AWS_DEFAULT_PROFILE = environ.get('AWS_DEFAULT_PROFILE')

    SERVICE_CONFIG_FILE = environ.get('TS_SERVICE_CONFIG_FILE')
    TRANSPORT_STREAM_BATCH_SIZE = environ.get('TRANSPORT_STREAM_BATCH_SIZE',
                                              32)

    with open(SERVICE_CONFIG_FILE, 'r') as f:
        config = yaml.safe_load(f)

    context = zmq.Context()

    print(AWS_DEFAULT_REGION, AWS_DEFAULT_PROFILE, SERVICE_CONFIG_FILE,
          TRANSPORT_STREAM_BATCH_SIZE)
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
                            tagged_addresses=config['elementary_streams']))),
                batch_size=TRANSPORT_STREAM_BATCH_SIZE,
            )

    elif config["mode"] == 'demux':
        fan_out = fan_out_tags(
            context, tagged_addresses=config['elementary_streams'])

        fan_out.send(None)

        while True:
            for record in yield_kinesis_records(
                    stream_name=config['stream_name'], client=kinesis_client):
                if record:
                    fan_out.send(json.loads(record))

    elif config["mode"] == 'raw':
        fan_out = fan_out_tags(context, raw_stream=config["raw_stream"])

        fan_out.send(None)

        while True:
            for record in yield_kinesis_records(
                    stream_name=config['stream_name'], client=kinesis_client):
                if record:
                    fan_out.send(record)

    elif config["mode"] == 'pub':

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
                        lambda x: x[1],
                        subscribe_and_tag(
                            context=context,
                            tagged_addresses=config['elementary_streams']))),
                batch_size=TRANSPORT_STREAM_BATCH_SIZE,
            )

    if config["mode"] == 'sump':

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
                        lambda x: f"{x[1]}\n",
                        subscribe_and_tag(
                            context=context,
                            tagged_addresses=config['elementary_streams'],
                            bind=True))),
                batch_size=TRANSPORT_STREAM_BATCH_SIZE,
            )

    else:
        raise Exception("unrecognized mode")
