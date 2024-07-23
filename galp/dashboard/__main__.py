"""
Simple dashboard local runner
"""

import argparse
from wsgiref.simple_server import make_server

import galp.cli

from . import create_app

def add_parser_arguments(_parser):
    """
    Add dashboard-specific options to server
    """
    _parser.add_argument('-p', '--port', help='Dashboard HTTP port', type=int,
            default=5000)

parser = argparse.ArgumentParser()
galp.store.add_store_argument(parser, optional=True)
galp.cli.add_parser_arguments(parser)
add_parser_arguments(parser)
args = parser.parse_args()
galp.cli.setup('dashboard', args.log_level)

app = create_app(**vars(args))

with make_server('', args.port, app) as httpd:
    print("Serving HTTP on port", args.port, flush=True)
    httpd.serve_forever()
