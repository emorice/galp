"""
Simple dashboard local runner
"""

import argparse
from wsgiref.simple_server import make_server

import galp.cli

from . import create_app

parser = argparse.ArgumentParser()
galp.cache.add_store_argument(parser, optional=True)
galp.cli.add_parser_arguments(parser)
args = parser.parse_args()
galp.cli.setup('dashboard', args.log_level)

app = create_app(vars(args))

with make_server('', 5000, app) as httpd:
    print("Serving HTTP on port 5000")
    httpd.serve_forever()
