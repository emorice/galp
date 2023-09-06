"""
Command line interface to pool
"""

import argparse

import galp.cli
import galp.worker
from .._pool import main

def add_parser_arguments(parser):
    """
    Pool-specific arguments, plus all arguments to pass on to workers
    """
    galp.worker.add_parser_arguments(parser)
    parser.add_argument('--pin_workers', action='store_true')
    parser.add_argument('--steps', action='append',
            help='Add the given python module to the set of usable steps')

_parser = argparse.ArgumentParser()
add_parser_arguments(_parser)

main(vars(_parser.parse_args()))
