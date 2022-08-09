"""
Command line interface to worker
"""

from .worker import main, make_parser

# Convenience hook to start a worker from CLI
main(make_parser().parse_args())
