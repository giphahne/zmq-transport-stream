import time
import json
from datetime import datetime

import boto3
import botocore.config
from botocore.exceptions import ClientError

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


def json_dumps(obj):
    return json.dumps(obj, cls=JSONItemEncoder, indent=4, sort_keys=True)


def get_kinesis_client(profile, region):
    """Instantiate a boto3 Kinesis client for the given region."""

    #config = botocore.config.Config()
    #config.region_name = region
    #config.connection_timeout = 60
    #config.read_timeout = 60
    session = boto3.Session(profile_name=profile)
    kinesis_client = session.client(
        'kinesis', region_name=region)  #, config=config)

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
                print("ReadthroughputExceed")
                # ReadthroughputExceed
                time.sleep(2)

        except ClientError as e:
            print("Error iterating Shards: {0}".format(e))
            break


def publish_to_kinesis(client, streamname, message_stream):
    """
    """

    Records = []
    for message in message_stream:
        Records.append({'Data': message, 'PartitionKey': 'ffs'})
        if len(Records) >= 3:
            client.put_records(StreamName=streamname, Records=Records)
            Records = []
