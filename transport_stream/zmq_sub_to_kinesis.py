import zmq
import argparse
import zlib

import boto3
import botocore.config
from botocore.exceptions import ClientError

def get_kinesis_client(client, region):

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


def publish_to_kinesis_and_firehose(streamname, region,
                                    message_stream):

    """Ingest the data to kinesis"""

    kinesis_client = get_kinesis_client('kinesis', region)
    Records = []
    count = 0
    for message in message_stream:
        Records.append({ 'Data': message,'PartitionKey': 'multilink'})
        count +=1
        if count == 500:
            kinesis_client.put_records(StreamName=streamname,
                                                          Records=Records)
            Records = []



def zmq_sub(context, tcp_address):


    socket = context.socket(zmq.SUB)
    socket.setsockopt_string(zmq.SUBSCRIBE, u"")
    socket.connect(tcp_address)
    while True:
        yield socket.recv()



if __name__ == '__main__':

    parser = argparse.ArgumentParser(
        description='Connects to zmq socket and insert data to kinesis')

    parser.add_argument(
        "--kinesisstreamname", help="kinesis stream name", type=str)

    parser.add_argument("--region", help="kinesis region", type=str)

    parser.add_argument("--zmq_address", help="hostname to connect", type=str)


    context = zmq.Context()
    publish_to_kinesis_and_firehose(streamname=args.kinesisstreamname,
                                    region=args.region,
                                    message_stream=zmq_sub(context, tcp_address=args.zmq_address))
