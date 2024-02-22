"""
Command line interface to the load-balancing broker
"""
import asyncio
import argparse
import logging

import galp.cli
from galp.broker import Broker
from galp.result import Error

async def main(args):
    """Entry point for the broker program

    Args:
        args: an object whose attributes are the the parsed arguments, as
            returned by argparse.ArgumentParser.parse_args.
    """
    galp.cli.setup("broker", args.log_level)

    broker = Broker(
        endpoint=args.endpoint,
        n_cpus=args.pool_size
        )

    match await broker.run():
        case Error() as err:
            logging.error('Borker error: %s', err)

def add_parser_arguments(parser):
    """Add broker-specific arguments to the given parser"""
    parser.add_argument(
        'endpoint',
        help="Endpoint to bind to, in ZMQ format, e.g. tcp://127.0.0.2:12345 "
            "or ipc://path/to/socket ; see also man zmq_bind. The broker will"
            "listen for incoming connections on this endpoint."
        )
    parser.add_argument('pool_size', type=int,
        help='Number of cpus to use')
    galp.cli.add_parser_arguments(parser)

_parser = argparse.ArgumentParser()
add_parser_arguments(_parser)
asyncio.run(main(_parser.parse_args()))
