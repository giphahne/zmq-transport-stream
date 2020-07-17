import time

import boto3
import botocore.config
from botocore.exceptions import ClientError


def get_kinesis_client(profile, region):
    """Create a boto3 Kinesis client for the given region."""

    config = botocore.config.Config()
    config.region_name = region
    config.connection_timeout = 60
    config.read_timeout = 60
    session = boto3.Session(profile_name=profile)
    kinesis_client = session.client(
        'kinesis', region_name=region, config=config)

    return kinesis_client


def yield_kinesis_records(client, stream_name, limit_number=500):

    def kinesis_shard_iterator(stream_name, client, IteratorType='LATEST'):
        response = client.describe_stream(StreamName=stream_name)
        #print(response['StreamDescription']['Shards'])
        return client.get_shard_iterator(
            StreamName=stream_name,
            ShardId=response['StreamDescription']['Shards'][0]['ShardId'],
            ShardIteratorType=IteratorType,
        )['ShardIterator']

    iterator = kinesis_shard_iterator(stream_name, client)

    record_response = client.get_records(
        ShardIterator=iterator, Limit=limit_number)

    while 'NextShardIterator' in record_response:
        try:
            record_response = client.get_records(
                ShardIterator=record_response['NextShardIterator'],
                Limit=limit_number)

            if len(record_response['Records']) > 0:
                for rcrd in range(len(record_response['Records'])):
                    yield record_response['Records'][rcrd]['Data']
            else:
                # ReadthroughputExceed
                time.sleep(0.2)

        except ClientError as e:
            print("Error in iterating Shards {0}".format(e))
            break


def publish_to_kinesis(client, streamname, message_stream):
    """Ingest the data to kinesis"""

    Records = []
    for message in message_stream:
        Records.append({'Data': message, 'PartitionKey': 'multilink'})
        if len(Records) >= 3:
            client.put_records(StreamName=streamname, Records=Records)
            Records = []
