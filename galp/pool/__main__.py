"""
Command line interface to pool
"""

import argparse
import asyncio
import logging
import signal

import galp.cli
import galp.worker
from .._pool import main


def add_parser_arguments(parser):
    """
    Pool-specific arguments, plus all arguments to pass on to workers
    """
    parser.add_argument('pool_size', type=int,
        help='Number of workers to start')
    galp.worker.add_parser_arguments(parser)

_parser = argparse.ArgumentParser()
add_parser_arguments(_parser)

main(vars(_parser.parse_args()))
