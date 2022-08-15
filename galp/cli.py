"""
Common CLI features used by several cli endpoints.
"""

import asyncio
import logging
import signal

def add_parser_arguments(parser):
    """Add generic arguments to the given parser"""
    parser.add_argument('--log-level',
        help='Turn on debug-level logging')

def setup(name, loglevel=None):
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
    level = dict(
        debug=logging.DEBUG,
        info=logging.INFO,
        warning=logging.WARNING
        ).get(loglevel, logging.WARNING)
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
        await cleanup_tasks(pending)
        for task in done:
            await task
    except:
        logging.exception("Aborting")
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
