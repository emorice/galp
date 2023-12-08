"""
Implementations of requests that simply read something straight from the
local store
"""

import logging

from typing import Iterable

from galp.messages import Get, Stat, NotFound, Found, Put, Done
from galp.cache import CacheStack, StoreReadError
from galp.protocol import Handler, make_reply_handler

def make_get_handler(store: CacheStack) -> Handler[Get]:
    """
    Answers GET requests based on underlying store
    """
    @make_reply_handler
    def _on_get(msg: Get):
        name = msg.name
        logging.debug('Received GET for %s', name)
        reply: Put | NotFound
        try:
            data, children = store.get_serial(name)
            reply = Put(name=name, data=data, children=children)
            logging.info('GET: Cache HIT: %s', name)
            return reply
        except KeyError:
            logging.info('GET: Cache MISS: %s', name)
        except StoreReadError:
            logging.exception('GET: Cache ERROR: %s', name)
        return NotFound(name=name)
    return Handler(Get, _on_get)

def make_stat_handler(store: CacheStack) -> Handler[Stat]:
    """
    Answers STAT requests based on underlying store
    """
    @make_reply_handler
    def _on_stat(msg: Stat):
        try:
            return _on_stat_io_unsafe(store, msg)
        except StoreReadError:
            logging.exception('STAT: ERROR %s', msg.name)
        return NotFound(name=msg.name)
    return Handler(Stat, _on_stat)

def _on_stat_io_unsafe(store, msg: Stat):
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
    return NotFound(name=msg.name)

def make_store_handlers(store: CacheStack) -> Iterable[Handler]:
    """
    Collect straight-to-store handlers
    """
    return [
        make_get_handler(store),
        make_stat_handler(store)
        ]
