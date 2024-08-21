"""
Command line interface to pool
"""

import argparse

import galp.cli
import galp.worker
from .._pool import main

_parser = argparse.ArgumentParser()
galp.worker.add_parser_arguments(_parser)

main(vars(_parser.parse_args()))
