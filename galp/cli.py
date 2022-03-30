"""
Common CLI features used by several cli endpoints.
"""

import asyncio
import logging
import signal
import sys

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
    log_format = (
        "%(asctime)s " # Time, useful when reading logs from overnight pipeline runs
        "%(levelname)s\t" # Level
        "%(name)s:%(filename)s:%(lineno)d\t" # Origin of log message inside process
        "["+name+" %(process)d] " # Identification of the process (type + pid)
        "%(message)s" # Message
    )
    if args.debug:
        logging.basicConfig(level=logging.DEBUG, format=log_format)
    else:
        logging.basicConfig(level=logging.INFO, format=log_format)

def log_signal(sig, context, orig_handler):
    logging.error("Caught signal %s", signal.strsignal(sig))
    if callable(orig_handler):
        return orig_handler(sig, context)
    else:
        logging.error('Re-raising signal', stack_info=context)
        signal.signal(sig, orig_handler)
        signal.raise_signal(sig)

def set_sync_handlers():
    for sig in (signal.SIGINT, signal.SIGTERM):
        orig_handler = signal.getsignal(sig)
        signal.signal(sig,
            lambda sig, context, orig=orig_handler: log_signal(sig, context, orig)
            )

def create_terminate():
    """
    Creates a terminate event with signal handler attached
    """
    terminate = asyncio.Event()

    # FIXME: graceful termination on signals

    #loop = asyncio.get_running_loop()
    #loop.add_signal_handler(signal.SIGINT, terminate.set)
    #loop.add_signal_handler(signal.SIGTERM, terminate.set)

    # Fallback
    set_sync_handlers()

    return terminate

async def wait(tasks):
    """
    Waits until any task finishes or raises, then cancels and awaits the others.

    Re-raises if needed.

    This is meant to be called with a list of the life-long tasks of your entry
    point. If any of them raises, you have an error in your app or an interrupt,
    and you want to re-raise it in the main task immediately. If any terminates,
    this means you have reached a termination condition, you want to cancel all
    the others and finish normally.
    """
    try:
        done, pending = await asyncio.wait(tasks,
            return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            await task
        await cleanup_tasks(pending)
    except:
        logging.error("Aborting")
        await cleanup_tasks(pending)
        raise
    else:
        logging.info("Terminating normally")

async def cleanup_tasks(tasks):
    """Cancels all tasks and wait for them"""
    for task in tasks:
        task.cancel()

    for task in tasks:
        try:
            await task
        except asyncio.CancelledError:
            pass
        # let other exceptions be raised
