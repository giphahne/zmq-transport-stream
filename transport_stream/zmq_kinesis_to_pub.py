import sys
import json
import time
import logging
import argparse
import zlib

import boto3
import botocore.config
from botocore.exceptions import ClientError


def get_kinesis_client(client, region='us-east-1'):
    """Create a boto3 Kinesis client for the given region."""

    config = botocore.config.Config()
    config.region_name = region
    config.connection_timeout = 60
    config.read_timeout = 60
    session = boto3.Session(profile_name='ffs-dev-eng')
    #logger.info("{0} client launched in region {1} ".format(client, region))
    kinesis_client = session.client(client, region_name=region, config=config)
    #     kinesis_client = boto3.client(
    #         client, region_name=region, config=config)  ## Kinesis Streams

    return kinesis_client


def shard_iterator(stream_name, client, IteratorType='LATEST'):

    response = client.describe_stream(StreamName=stream_name)
    # print(response)
    #logger.info("Response message for stream {0} : {1}".format(
    #stream_name, response))
    print(response['StreamDescription']['Shards'])
    shard_id = response['StreamDescription']['Shards'][0]['ShardId']

    shard_iterator = client.get_shard_iterator(
        StreamName=stream_name,
        ShardId=shard_id,
        ShardIteratorType=IteratorType,
        )

    return shard_iterator['ShardIterator']


def get_records(stream_name, client, limit_number=500):

    iterator = shard_iterator(stream_name, client)

    record_response = client.get_records(ShardIterator=iterator,
                                         Limit=limit_number)

    while 'NextShardIterator' in record_response:
        try:
            record_response = client.get_records(
                ShardIterator=record_response['NextShardIterator'],
                Limit=limit_number)

            if len(record_response['Records']) == 0:

                time.sleep(0.2)  ## ReadthroughputExceed
            else:
                for rcrd in range(len(record_response['Records'])):
                    yield record_response['Records'][rcrd]['Data']

        except ClientError as e:
            #logger.error("Error in iterating Shards {0}".format(e))
            break


if __name__ == '__main__':

    streamname = 'KinesisResources-tfmstream-7N20AOHH8PWE'
    region = 'us-east-1'
    kinesis_client = get_kinesis_client('kinesis', region)
    #     tcp_address = "tcp://127.0.0.1:5680"

    while True:
        for record in get_records(stream_name=streamname,
                                  client=kinesis_client):
            if record:
                print(json.loads(record))
