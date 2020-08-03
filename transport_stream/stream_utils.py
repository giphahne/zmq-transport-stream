import time
import json
from datetime import datetime

import zmq
import boto3
import botocore.config
from botocore.exceptions import ClientError

from kinesis.consumer import KinesisConsumer

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
    return json_dumps(message)


def decode_elementary_stream_message(message):
    return json.loads(message)


def subscribe_and_tag(context, tagged_addresses, subscriptions=""):
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
        print("connecting to {}, with tag: {}".format(address, tag))
        socket = context.socket(zmq.SUB)

        for subscription in subscriptions:
            try:
                socket.setsockopt_string(zmq.SUBSCRIBE, unicode(subscription))
            except NameError:
                socket.setsockopt_string(zmq.SUBSCRIBE, subscription)

        socket.connect(address)
        poller.register(socket, zmq.POLLIN)
        tag_dict[socket] = tag

    while True:
        events = poller.poll()
        for socket, _ in events:
            yield (tag_dict[socket], socket.recv().decode())


def fan_out_tags(context, tagged_addresses):
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

    for tag, address in tagged_addresses:
        print("binding to {}, with tag: {}".format(address, tag))
        socket = context.socket(zmq.PUB)

        socket.bind(address)
        tag_dict[tag] = socket

    while True:
        message = yield
        tag_dict[message[0]].send(message[1].encode())


def get_kinesis_client(profile, region):
    """Instantiate a boto3 Kinesis client for the given region."""

    session = boto3.Session(profile_name=profile)
    kinesis_client = session.client('kinesis', region_name=region)

    return kinesis_client


def yield_kinesis_records(client, stream_name):
    """
    """

    def kinesis_shard_iterator(stream_name, client, IteratorType='LATEST'):
        response = client.describe_stream(StreamName=stream_name)
        print(json_dumps(response))
        return client.get_shard_iterator(
            StreamName=stream_name,
            ShardId=response['StreamDescription']['Shards'][0]['ShardId'],
            ShardIteratorType=IteratorType,
        )['ShardIterator']

    iterator = kinesis_shard_iterator(stream_name, client)

    record_response = client.get_records(ShardIterator=iterator)

    while 'NextShardIterator' in record_response:
        try:
            record_response = client.get_records(
                ShardIterator=record_response['NextShardIterator'])

            if record_response['Records']:
                for record in record_response['Records']:
                    yield record['Data']
            else:
                #print("ReadthroughputExceed")
                #print(json_dumps(record_response))
                # ReadthroughputExceed
                time.sleep(0.33333)

        except ClientError as e:
            print("Error iterating Shards: {0}".format(e))
            break


# def yield_kinesis_records(client, stream_name):
#     """
#     """
#     consumer = KinesisConsumer(stream_name=stream_name)
#     for message in consumer:
#         yield message


def publish_to_kinesis(client, streamname, message_stream):
    """
    """

    Records = []
    for message in message_stream:
        Records.append({'Data': message, 'PartitionKey': 'ffs'})
        if len(Records) >= 3:
            client.put_records(StreamName=streamname, Records=Records)
            Records = []
