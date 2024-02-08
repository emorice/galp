"""
Implementations of requests that simply read something straight from the
local store
"""

import logging

from typing import Iterable

from galp.net.core.types import Get, Stat, Message
from galp.net.core.dump import Writer
from galp.net.requests.types import NotFound, Found, Put, Done
from galp.net.core.dump import add_request_id
from galp.cache import CacheStack, StoreReadError
from galp.req_rep import Handler

def make_get_handler(store: CacheStack) -> Handler[Get]:
    """
    Answers GET requests based on underlying store
    """
    def _on_get(write: Writer[Message], msg: Get):
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
        return [write_reply(NotFound())]
    return Handler(Get, _on_get)

def make_stat_handler(store: CacheStack) -> Handler[Stat]:
    """
    Answers STAT requests based on underlying store
    """
    def _on_stat(write: Writer[Message], msg: Stat):
        write_reply = add_request_id(write, msg)
        try:
            return [write_reply(_on_stat_io_unsafe(store, msg))]
        except StoreReadError:
            logging.exception('STAT: ERROR %s', msg.name)
        return [write_reply(NotFound())]
    return Handler(Stat, _on_stat)

def _on_stat_io_unsafe(store, msg: Stat) -> Done | Found | NotFound:
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
        return Done(task_def=task_def, result=result_ref)

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

def make_store_handlers(store: CacheStack) -> Iterable[Handler]:
    """
    Collect straight-to-store handlers
    """
    return [
        make_get_handler(store),
        make_stat_handler(store)
        ]
