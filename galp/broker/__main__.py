"""
Command line interface to the load-balancing broker
"""
import asyncio
import argparse

import galp.cli
from galp.broker.broker import Broker

async def main(args):
    """Entry point for the broker program

    Args:
        args: an object whose attributes are the the parsed arguments, as
            returned by argparse.ArgumentParser.parse_args.
    """
    galp.cli.setup("broker", args.debug)

    broker = Broker(
        client_endpoint=args.client_endpoint,
        worker_endpoint=args.worker_endpoint
        )

    await broker.run()

def add_parser_arguments(parser):
    """Add broker-specific arguments to the given parser"""
    parser.add_argument(
        'client_endpoint',
        help="Endpoint to bind to, in ZMQ format, e.g. tcp://127.0.0.2:12345 "
            "or ipc://path/to/socket ; see also man zmq_bind. The broker will"
            "listen for incoming client connections on this endpoint."
        )
    parser.add_argument(
        'worker_endpoint',
        help="Endpoint to bind to, in ZMQ format, e.g. tcp://127.0.0.2:12345 "
            "or ipc://path/to/socket ; see also man zmq_bind. The broker will"
            "listen for incoming worker connections on this endpoint."
        )

    galp.cli.add_parser_arguments(parser)

_parser = argparse.ArgumentParser()
add_parser_arguments(_parser)
asyncio.run(main(_parser.parse_args()))
