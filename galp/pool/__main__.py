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

_parser = argparse.ArgumentParser()
add_parser_arguments(_parser)

main(vars(_parser.parse_args()))
