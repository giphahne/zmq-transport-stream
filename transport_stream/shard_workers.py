"""
Read every open shard of a Kinesis stream concurrently and fan the records
out to ZMQ from a single writer process.

    shard reader (process) --\\
    shard reader (process) ----> multiprocessing.Queue --> ZMQ writer (process)
    shard reader (process) --/

The parent process supervises: it starts one reader per open shard, restarts
readers that die, picks up new shards after a reshard, and shuts everything
down if the writer exits.

Processes are started with the "spawn" method so that no boto3 client or
zmq.Context is ever shared across a fork; each process builds its own.
"""

import json
import multiprocessing
import multiprocessing.connection
import signal
import sys
import time
import traceback

import zmq

from transport_stream.stream_utils import fan_out_tags
from transport_stream.stream_utils import get_consumer_arn
from transport_stream.stream_utils import get_kinesis_client
from transport_stream.stream_utils import list_open_shard_ids
from transport_stream.stream_utils import yield_kinesis_records
from transport_stream.stream_utils import yield_shard_event_records

SHARD_REFRESH_INTERVAL = 30
DEFAULT_QUEUE_MAXSIZE = 10000


def shard_reader(
    queue,
    shard_id,
    mode,
    kinesis_source_type,
    stream_name,
    stream_arn,
    consumer_arn,
    profile,
    region,
    verbose,
):
    """
    Read one shard forever, putting decoded records on ``queue``.  Returns
    once the shard has been closed (e.g. after a reshard).
    """

    client = get_kinesis_client(profile, region)

    if kinesis_source_type == "ENHANCED_FAN_OUT":
        read_shard = lambda: yield_shard_event_records(
            client=client,
            stream_arn=stream_arn,
            stream_name=stream_name,
            shard_id=shard_id,
            consumer_arn=consumer_arn,
        )
    else:
        read_shard = lambda: yield_kinesis_records(
            client=client,
            stream_name=stream_name,
            stream_arn=stream_arn,
            shard_id=shard_id,
        )

    while True:
        try:
            for record in read_shard():
                if not record:
                    continue

                if verbose:
                    print(f"[{shard_id}] {record}")

                if mode == "demux":
                    queue.put(json.loads(json.loads(record.decode())))
                else:
                    queue.put(record)

        except Exception:
            print(f"[{shard_id}] error reading shard:")
            traceback.print_exc()
            sys.stdout.flush()
            time.sleep(1)

        # An EFO subscription lasts ~5 minutes and a polling iterator ends when
        # the shard closes; only re-subscribe if the shard is still open.
        if shard_id not in list_open_shard_ids(client, stream_name):
            print(f"[{shard_id}] shard closed, reader exiting")
            return


def zmq_writer(queue, mode, elementary_streams, raw_stream):
    """Pull records off ``queue`` and publish them on the ZMQ output socket(s)."""

    context = zmq.Context()

    if mode == "demux":
        fan_out = fan_out_tags(context, tagged_addresses=elementary_streams)
    else:
        fan_out = fan_out_tags(context, raw_stream=raw_stream)

    # advance the fan_out_tags generator to the ``yield '' statement
    fan_out.send(None)

    while True:
        fan_out.send(queue.get())


def run_multi_shard(config, profile, region, verbose=False):
    """Supervise one reader process per open shard plus a single writer process."""

    mode = config["mode"]
    stream_name = config["stream_name"]
    stream_arn = config.get("stream_arn")
    kinesis_source_type = config.get("kinesis_source_type", "ENHANCED_FAN_OUT")
    refresh_interval = config.get("shard_refresh_interval", SHARD_REFRESH_INTERVAL)

    client = get_kinesis_client(profile, region)

    consumer_arn = None
    if kinesis_source_type == "ENHANCED_FAN_OUT":
        # register once here rather than racing to register from every reader
        consumer_arn = get_consumer_arn(client, stream_arn)

    ctx = multiprocessing.get_context("spawn")
    queue = ctx.Queue(maxsize=config.get("queue_maxsize", DEFAULT_QUEUE_MAXSIZE))

    writer = ctx.Process(
        target=zmq_writer,
        name="zmq-writer",
        args=(
            queue,
            mode,
            config.get("elementary_streams", []),
            config.get("raw_stream"),
        ),
        daemon=True,
    )
    writer.start()

    readers = {}

    def shutdown(*_):
        raise SystemExit(0)

    signal.signal(signal.SIGTERM, shutdown)

    try:
        while True:
            if not writer.is_alive():
                raise RuntimeError(f"zmq writer exited with code {writer.exitcode}")

            for shard_id, reader in list(readers.items()):
                if not reader.is_alive():
                    print(f"reader for {shard_id} exited with code {reader.exitcode}")
                    del readers[shard_id]

            for shard_id in list_open_shard_ids(client, stream_name):
                if shard_id in readers:
                    continue

                print(f"starting reader for {shard_id}")
                reader = ctx.Process(
                    target=shard_reader,
                    name=f"reader-{shard_id}",
                    args=(
                        queue,
                        shard_id,
                        mode,
                        kinesis_source_type,
                        stream_name,
                        stream_arn,
                        consumer_arn,
                        profile,
                        region,
                        verbose,
                    ),
                    daemon=True,
                )
                reader.start()
                readers[shard_id] = reader

            sys.stdout.flush()

            # wake early if any child exits so it can be replaced promptly
            multiprocessing.connection.wait(
                [writer.sentinel] + [r.sentinel for r in readers.values()],
                timeout=refresh_interval,
            )
            # avoid a tight restart loop (and DescribeStream throttling) if a
            # reader keeps dying immediately
            time.sleep(1)

    finally:
        for process in [writer, *readers.values()]:
            if process.is_alive():
                process.terminate()
        for process in [writer, *readers.values()]:
            process.join(timeout=5)
