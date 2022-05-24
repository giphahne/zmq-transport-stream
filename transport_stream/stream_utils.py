import time
import json
from datetime import datetime
import hashlib
import zlib
import gzip
import os
from functools import partial
import base64
import argparse
import fileinput
import sys
import binascii
import platform

import zmq
import boto3
import botocore.config
from botocore.exceptions import ClientError
import botocore.session
from botocore import credentials

import aws_kinesis_agg.aggregator
from aws_kinesis_agg.deaggregator import iter_deaggregate_records

TIMESTAMP_FORMAT = '%Y-%m-%d %H:%M:%S.%fZ'
TIMEONLY_FORMAT = '%H:%M:%S'


class JSONItemEncoder(json.JSONEncoder):

    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.strftime(TIMESTAMP_FORMAT)

        elif isinstance(obj, time):
            return obj.strftime(TIMEONLY_FORMAT)

        elif isinstance(obj, deque):
            return list(obj)

        return json.JSONEncoder.default(self, obj)


def json_dumps(obj, indent=None, sort_keys=False):
    return json.dumps(
        obj, cls=JSONItemEncoder, indent=indent, sort_keys=sort_keys)


def encode_elementary_stream_message(message):
    return json_dumps(message) + "\n"


def decode_elementary_stream_message(message):
    return json.loads(message)


def subscribe_and_tag(context, tagged_addresses, subscriptions="", bind=False):
    """This allows you to subscribe to one or more addresses and subscribe
    to multiple 'feeds'. The generator returns a tuple.  The first is
    the tag that is associated with the address the message came from.
    The second element is the message itself.

    This is an infinite generator.  It will never finish.

    @param <zmq.Context> context: a zmq context.

    @param <tuple(s)> tagged_addresses: a list of tuples. The tuple a
                                        tag and an address. The address
                                        is a string representing address
                                        to which to connect. The tag is
                                        the the thing that will be
                                        attached to messages that come
                                        from the address.
    @param <string(s)> subscriptions: a string of iterable of strings
                                      to which to 'subscribe'

    """
    if isinstance(subscriptions, str):
        subscriptions = [subscriptions]

    poller = zmq.Poller()
    tag_dict = {}

    for tag, address in tagged_addresses:

        if bind:
            print("binding to {}, with tag: {}".format(address, tag))
        else:
            print("connecting to {}, with tag: {}".format(address, tag))

        socket = context.socket(zmq.SUB)

        for subscription in subscriptions:
            try:
                socket.setsockopt_string(zmq.SUBSCRIBE, unicode(subscription))
            except NameError:
                socket.setsockopt_string(zmq.SUBSCRIBE, subscription)

        socket.setsockopt(zmq.RCVHWM, 90000)

        if bind:
            socket.bind(address)
        else:
            socket.connect(address)
        poller.register(socket, zmq.POLLIN)
        tag_dict[socket] = tag

    while True:
        events = poller.poll()
        for socket, _ in events:
            yield (tag_dict[socket], socket.recv().decode())


def fan_out_tags(context, tagged_addresses=[], raw_stream=None):
    """
                      <zmq address>
                    /
                   /
    generator -->  -- <zmq address>
                   \
                    \
                      <zmq address>
    """
    tag_dict = {}

    if raw_stream:
        print("binding RAW stream to {}".format(raw_stream))
        socket = context.socket(zmq.PUB)

        socket.bind(raw_stream)
        tag_dict["raw"] = socket

    else:
        for tag, address in tagged_addresses:
            print("binding to {}, with tag: {}".format(address, tag))
            socket = context.socket(zmq.PUB)

            socket.bind(address)
            tag_dict[tag] = socket

    while True:
        message = yield
        if raw_stream:
            tag_dict["raw"].send(message)

        else:
            #print(message[0])
            #print(message[1])
            #print(message)
            tag_dict[message[0]].send(message[1].encode())


def get_kinesis_client(profile, region):
    """Instantiate a boto3 Kinesis client for the given region."""

    cli_cache = os.path.join(os.path.expanduser('~'), '.aws/boto/cache')

    # Construct botocore session with cache
    session = botocore.session.get_session()
    session.get_component('credential_provider').get_provider(
        'assume-role').cache = credentials.JSONFileCache(cli_cache)

    return boto3.Session(
        botocore_session=session, profile_name=profile).client(
            'kinesis', region_name=region)


def de_aggregate_records(stream_record):
    pass


def package_raw_record_for_deaggregation(stream_record):
    """
    {
        "Records": [
            {
                "kinesis": {
                    "kinesisSchemaVersion": "1.0",
                    "partitionKey": "1",
                    "sequenceNumber": "495903382714906588898...",
                    "data": "SGVsbG8sIHRoaXMgaXMgYSB0ZXN0Lg==",
                    "approximateArrivalTimestamp": 1545084650.987
                },
                "eventSource": "aws:kinesis",
                "eventVersion": "1.0",
                "eventID": "shardId-000000000006:4959033827989136588898...",
                "eventName": "aws:kinesis:record",
                "invokeIdentityArn": "<ARN>",
                "awsRegion": "us-east-2",
                "eventSourceARN": "<ARN>"
            },
            ...
        ]
    }
    """
    return {
        "kinesis": {
            "kinesisSchemaVersion": "1.0",
            #"partitionKey": "1",
            "sequenceNumber": "<stub>",
            #"data": stream_record['Data'],
            "data": base64.b64encode(stream_record['Data']),
            "approximateArrivalTimestamp": 0
        },
        #        "kinesis": stream_record,
        #"eventSource": "aws:kinesis",
        #"eventVersion": "1.0",
        #"eventID": "shardId-000000000006:4959033827989136588898...",
        #"eventName": "aws:kinesis:record",
        #"invokeIdentityArn": "<ARN>",
        #"awsRegion": "us-east-2",
        #"eventSourceARN": "<ARN>"
    }


def un_pack_deaggregated_record(user_record):
    return base64.b64decode(user_record['kinesis']['data'])


def yield_kinesis_records(client, stream_name, stream_arn=None):
    """
    """

    def kinesis_shard_iterator(stream_name,
                               client,
                               IteratorType='LATEST',
                               stream_arn=None):
        response = client.describe_stream(StreamName=stream_name)

        print(f"describe stream: {json_dumps(response, indent=4)}")

        return client.get_shard_iterator(
            StreamName=stream_name,
            ShardId=response['StreamDescription']['Shards'][-1]['ShardId'],
            ShardIteratorType=IteratorType,
        )['ShardIterator']

    iterator = kinesis_shard_iterator(
        stream_name=stream_name, client=client, stream_arn=stream_arn)

    record_response = client.get_records(ShardIterator=iterator)

    while 'NextShardIterator' in record_response:
        try:
            record_response = client.get_records(
                ShardIterator=record_response['NextShardIterator'])

            if record_response['Records']:
                for user_record in map(
                        un_pack_deaggregated_record,
                        iter_deaggregate_records(
                            map(package_raw_record_for_deaggregation,
                                record_response['Records']))):

                    try:
                        yield gzip.decompress(base64.b64decode(user_record))
                    except (gzip.BadGzipFile, binascii.Error) as e:
                        yield user_record

            else:
                # ReadthroughputExceed
                time.sleep(0.33333)

        except ClientError as e:
            print("Error iterating Shards: {0}".format(e))
            break


def yield_shard_event_records(client, stream_arn, stream_name=None):
    """
    Stream using Enhanced Fan Out
    """

    def kinesis_shard_event_iterator(client,
                                     stream_arn=None,
                                     stream_name=None):
        describe_response = client.describe_stream(StreamName=stream_name)
        shard_id = describe_response["StreamDescription"]["Shards"][0][
            "ShardId"]
        print(f"describe stream: {json_dumps(describe_response, indent=4)}")

        print(f"consumer name: {platform.node()}")

        try:
            registeration_response = client.register_stream_consumer(
                StreamARN=stream_arn, ConsumerName=platform.node())

            print(
                f"register consumer response: {json_dumps(response, indent=4)}"
            )

            consumer_arn = registeration_response["Consumer"]["ConsumerARN"]
        except:
            print("consumer alread exists, fetching ARN...")
            consumer_arn = client.describe_stream_consumer(
                StreamARN=stream_arn, ConsumerName=platform.node()
            )['ConsumerDescription']['ConsumerARN']
            print(f"consumerARN: {consumer_arn}")

        response = client.subscribe_to_shard(
            ConsumerARN=consumer_arn,
            ShardId=shard_id,
            StartingPosition={'Type': 'LATEST'})

        return response['EventStream']

    event_stream = kinesis_shard_event_iterator(
        stream_name=stream_name, client=client, stream_arn=stream_arn)

    for event in event_stream:
        if 'SubscribeToShardEvent' not in event:
            print(event)
            raise Exception
        else:
            for user_record in map(
                    un_pack_deaggregated_record,
                    iter_deaggregate_records(
                        map(package_raw_record_for_deaggregation,
                            event['SubscribeToShardEvent']['Records']))):
                try:
                    yield gzip.decompress(base64.b64decode(user_record))
                except (gzip.BadGzipFile, binascii.Error) as e:
                    yield user_record


def base_send_agg_record(client, streamname, agg_record=None):
    pk, ehk, data = agg_record.get_contents()
    print(f"sending: {pk}")

    client.put_record(StreamName=streamname, Data=data, PartitionKey=pk)


def publish_to_kinesis(client, streamname, message_stream, batch_size=None):
    """
    """

    kinesis_aggregator = aws_kinesis_agg.aggregator.RecordAggregator()

    send_agg_record = partial(base_send_agg_record, client, streamname)
    kinesis_aggregator.on_record_complete(send_agg_record)

    for count, message in enumerate(message_stream):
        pk, ehk, data = (
            hashlib.md5(str(message).encode('utf-8')).hexdigest(), None,
            base64.b64encode(
                gzip.compress(
                    json_dumps(message).encode('utf-8'))).decode('utf-8') +
            '\n')

        kinesis_aggregator.add_user_record(pk, data, ehk)

        if batch_size is not None and count % int(batch_size) == 0:
            print("batch size reached, sending.")
            send_agg_record(kinesis_aggregator.clear_and_get())

    #Clear out any remaining records that didn't trigger a callback yet
    print("clearing out extra:")
    send_agg_record(kinesis_aggregator.clear_and_get())


def unpack_stream_record():
    """
    Entry point for CLI application.
    """

    description = "Unpack Aggregated Kinesis Records"
    parser = argparse.ArgumentParser(usage=None, description=description)

    parser.add_argument("-f", "--files", type=str, nargs="*", default="-")

    args = parser.parse_args()

    try:
        with fileinput.input(args.files) as f:
            for l in f:
                sys.stdout.write(
                    gzip.decompress(base64.b64decode(l)).decode('utf-8'))
                sys.stdout.write('\n')

    except (BrokenPipeError, IOError, StopIteration) as e:
        pass

    sys.stderr.close()
