"""
Implementations of requests that simply read something straight from the
local store
"""

import logging

from typing import Iterable

import galp.commands as cm
from galp.net.core.types import Get, Stat, Message
from galp.net.core.dump import Writer
from galp.net.requests.types import NotFound, Found, Put, StatDone, GetNotFound
from galp.net.core.dump import add_request_id
from galp.cache import CacheStack, StoreReadError
from galp.protocol import TransportMessage
from galp.result import Error

def handle_get(write: Writer[Message], msg: Get, store: CacheStack
        ) -> Iterable[TransportMessage]:
    """
    Answers GET requests based on underlying store
    """
    write_reply = add_request_id(write, msg)
    name = msg.name
    logging.debug('Received GET for %s', name)
    try:
        res = Put(*store.get_serial(name))
        logging.info('GET: Cache HIT: %s', name)
        return [write_reply(res)]
    except KeyError:
        logging.info('GET: Cache MISS: %s', name)
    except StoreReadError:
        logging.exception('GET: Cache ERROR: %s', name)
    return [write_reply(GetNotFound())]

def handle_stat(write: Writer[Message], msg: Stat, store: CacheStack
        ) -> Iterable[TransportMessage]:
    """
    Answers STAT requests based on underlying store
    """
    write_reply = add_request_id(write, msg)
    try:
        return [write_reply(_on_stat_io_unsafe(store, msg))]
    except StoreReadError:
        logging.exception('STAT: ERROR %s', msg.name)
    return [write_reply(NotFound())]

def _on_stat_io_unsafe(store, msg: Stat) -> StatDone | Found | NotFound:
    """
    STAT handler allowed to raise store read errors
    """
    # Try first to extract both definition and children
    task_def = None
    try:
        task_def = store.get_task_def(msg.name)
    except KeyError:
        pass

    result_ref = None
    try:
        result_ref = store.get_children(msg.name)
    except KeyError:
        pass

    # Case 1: both def and children, DONE
    if task_def is not None and result_ref is not None:
        logging.info('STAT: DONE %s', msg.name)
        return StatDone(task_def=task_def, result=result_ref)

    # Case 2: only def, FOUND
    if task_def is not None:
        logging.info('STAT: FOUND %s', msg.name)
        return Found(task_def=task_def)

    # Case 3: only children
    # This means a legacy store that was missing tasks definition
    # persistency, or a corruption. This was originally treated as DONE, but
    # in the current model there is no way to make the peer accept a missing
    # or fake definition
    # if children is not None:

    # Case 4: nothing
    logging.info('STAT: NOT FOUND %s', msg.name)
    return NotFound()

# Reply adapters
# ==============
# Probably not needed on the long run

def on_stat_reply(stat_command, msg: Found | StatDone | NotFound) -> list[cm.InertCommand]:
    """Handle stat replies"""
    return stat_command.done(msg)

def on_get_reply(get_command, msg: Put | GetNotFound) -> list[cm.InertCommand]:
    """
    Handle get replies
    """
    match msg:
        case Put():
            return get_command.done(msg)
        case GetNotFound():
            logging.error('TASK RESULT FETCH FAILED: %s', get_command.name)
            return get_command.failed(Error('NOTFOUND'))
