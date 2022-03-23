"""
Common CLI features used by several cli endpoints.
"""

import asyncio
import logging
import signal

class IllegalRequestError(Exception):
    """Base class for all badly formed requests, triggers sending an ILLEGAL
    message back"""
    def __init__(self, route, reason):
        self.route = route
        self.reason = reason

def add_parser_arguments(parser):
    """Add generic arguments to the given parser"""
    parser.add_argument('-d', '--debug', action='store_true',
        help='Turn on debug-level logging')

def setup(args, name):
    """
    Common CLI setup steps
    """
    log_format = "%(levelname)s\t%(name)s:%(filename)s:%(lineno)d\t["+name+"] %(message)s"
    if args.debug:
        logging.basicConfig(level=logging.DEBUG, format=log_format)
    else:
        logging.basicConfig(level=logging.INFO, format=log_format)

def create_terminate():
    """
    Creates a terminate event with signal handler attached
    """
    terminate = asyncio.Event()
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGINT, terminate.set)
    loop.add_signal_handler(signal.SIGTERM, terminate.set)

    return terminate

async def cleanup_tasks(tasks):
    for task in tasks:
        task.cancel()

    for task in tasks:
        try:
            await task
        except asyncio.CancelledError:
            pass
        # let other exceptions be raised
