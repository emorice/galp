"""
Command line interface to worker
"""

from .._worker import main, make_parser

# Convenience hook to start a worker from CLI
main(vars(make_parser().parse_args()))
