"""
Command line interface to worker
"""

from .worker import main, make_parser, make_config

# Convenience hook to start a worker from CLI
main(
    make_config(
        make_parser().parse_args()
        )
    )
