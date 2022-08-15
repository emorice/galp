"""
Command line interface to pool
"""

import argparse
import asyncio
import logging
import signal

import galp.cli
import galp.worker
from .pool import Pool


def on_signal(sig, pool):
    """
    Signal handler, propagates it to the pool

    Does not handle race conditions where a signal is received before the
    previous is handled, but that should not be a problem: CHLD always does the
    same thing and TERM or INT would supersede CHLD.
    """
    logging.info("Caught signal %d (%s)", sig, signal.strsignal(sig))
    pool.set_signal(sig)

async def main(args):
    """
    Main CLI entry point
    """
    galp.cli.setup(" pool ", args.log_level)
    logging.info("Starting worker pool")

    config = {
        'pool_size': args.pool_size,
        'endpoint': args.endpoint,
        }

    pool = Pool(config, galp.worker.worker.make_config(args))

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGCHLD):
        loop.add_signal_handler(sig,
            lambda sig=sig, pool=pool: on_signal(sig, pool)
        )

    await pool.run()

    logging.info("Pool manager exiting")

def add_parser_arguments(parser):
    """
    Pool-specific arguments
    """
    parser.add_argument('pool_size', type=int,
        help='Number of workers to start')
    parser.add_argument('--restart_delay', type=int,
        help='Do not restart more than one failed worker '
        'per this interval of time (s)',
        default=3600
        )
    parser.add_argument('--min_restart_delay', type=int,
        help='Wait at least this time (s) before restarting a failed worker',
        default=300
        )
    galp.worker.add_parser_arguments(parser)

_parser = argparse.ArgumentParser()
add_parser_arguments(_parser)
asyncio.run(main(_parser.parse_args()))
