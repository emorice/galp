"""
Common CLI features used by several cli endpoints.
"""

from typing import Iterable

import os
import asyncio
import logging
import signal

import galp.config
from galp.result import Result, Ok, Error

def add_parser_arguments(parser):
    """Add generic arguments to the given parser"""
    parser.add_argument('--log-level',
        help='specify logging level (warning, info, debug)')
    galp.config.add_config_argument(parser)

def setup(name, loglevel=None):
    """
    Common CLI setup steps

    Args:
        name: a string that will be added to each log line, identifying the
            process type
        loglevel: a string among 'debug', 'info', 'warn' (case-insenstivie), or an integer or
            integer-like string, or None (= warn)
    """
    log_format = (
        "%(asctime)s " # Time, useful when reading logs from overnight pipeline runs
        "%(levelname)s\t" # Level
        "%(name)s:%(filename)s:%(lineno)d\t" # Origin of log message inside process
        "["+name+" %(process)d] " # Identification of the process (type + pid)
        "%(message)s" # Message
    )
    if loglevel is None:
        loglevel = logging.WARNING
    try:
        level = int(loglevel)
    except ValueError:
        loglevel = loglevel.lower()
        level = {
            'debug': logging.DEBUG,
            'info': logging.INFO,
            'warning': logging.WARNING
            }.get(loglevel)
    # Note the use of force ; this unregisters existing handlers that could have been
    # set by a parent forking process
    logging.basicConfig(level=level, format=log_format, force=True)

def log_signal(sig, context, orig_handler):
    """
    Logs the signal received and re-raises it
    """
    logging.info("Caught signal %s", signal.strsignal(sig))
    if callable(orig_handler):
        return orig_handler(sig, context)

    logging.info("Re-raising signal %s", signal.strsignal(sig))
    logging.debug('Re-raising signal from', stack_info=context)
    signal.signal(sig, orig_handler)
    return signal.raise_signal(sig)

def set_sync_handlers(reset=True):
    """
    Add a logging hook to signal handlers, chaining either the pre-existing or
    default handlers.

    Resetting handlers can be important after a fork since the parent may have
    defined handlers that make no sense for the child.
    """
    for sig in (signal.SIGINT, signal.SIGTERM):
        if reset:
            if sig == signal.SIGINT:
                orig_handler = signal.default_int_handler
            else:
                orig_handler = signal.Handlers.SIG_DFL
        else:
            orig_handler = signal.getsignal(sig)
        signal.signal(sig,
            lambda sig, context, orig=orig_handler: log_signal(sig, context, orig)
            )

def create_terminate():
    """
    Creates a terminate event with signal handler attached
    """
    terminate = asyncio.Event()

    # Issue #7: graceful termination on signals

    #loop = asyncio.get_running_loop()
    #loop.add_signal_handler(signal.SIGINT, terminate.set)
    #loop.add_signal_handler(signal.SIGTERM, terminate.set)

    # Fallback
    set_sync_handlers()

    return terminate

async def wait(tasks: Iterable[asyncio.Task[None | Error]]):
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
        await cleanup_tasks(pending)
        for task in done:
            match await task:
                case None:
                    continue
                case Error() as err:
                    logging.error("App coroutine returned error:\n%s", err)
    except:
        logging.exception("Aborting")
        raise
    logging.info("Terminating process normally")

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

def run_in_fork(function, *args, **kwargs):
    """
    Functional Wrapper for fork including exception handling and clean-up
    """
    pid = os.fork()
    if pid == 0:
        ret = 1
        try:
            ret = function(*args, **kwargs)
        except:
            # os._exit swallows exception so make sure to log them
            logging.exception('Uncaught exception in forked process')
            raise
        finally:
            # Not sys.exit after a fork as it could call parent-specific
            # callbacks
            os._exit(ret) # pylint: disable=protected-access
    return pid
