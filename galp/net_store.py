"""
Implementations of requests that simply read something straight from the
local store
"""

import logging

from typing import Iterable

from galp.result import Ok
from galp.net.core.types import Get, Stat, Upload, Message
from galp.net.core.dump import Writer
from galp.net.requests.types import StatResult, RemoteError
from galp.net.core.dump import add_request_id
from galp.store import Store, StoreReadError
from galp.protocol import TransportMessage

def handle_get(write: Writer[Message], msg: Get, store: Store
        ) -> Iterable[TransportMessage]:
    """
    Answers GET requests based on underlying store
    """
    write_reply = add_request_id(write, msg)
    name = msg.name
    logging.debug('Received GET for %s', name)
    try:
        res = store.get_serial(name)
        logging.info('GET: Cache HIT: %s', name)
        return [write_reply(Ok(res))]
    except KeyError:
        err = 'Object not found in store'
        logging.error('GET: %s: %s', err, name)
    except StoreReadError:
        err = 'Error when trying to read object from store, see worker logs'
        logging.exception('GET: %s: %s', err, name)
    return [write_reply(RemoteError(err))]

def handle_stat(write: Writer[Message], msg: Stat, store: Store
        ) -> Iterable[TransportMessage]:
    """
    Answers STAT requests based on underlying store
    """
    write_reply = add_request_id(write, msg)
    try:
        return [write_reply(Ok(_on_stat_io_unsafe(store, msg)))]
    except StoreReadError:
        err = 'Error when trying to read object from store, see worker logs'
        logging.exception('STAT: %s: %s', err, msg.name)
    return [write_reply(RemoteError(err))]

def handle_upload(write: Writer[Message], msg: Upload, store: Store
                  ) -> Iterable[TransportMessage]:
    """
    Check object into store
    """
    write_reply = add_request_id(write, msg)
    store.put_task_def(msg.task_def)
    result_ref = store.put_serial(msg.task_def.name, msg.payload)
    return [write_reply(Ok(result_ref))]

def _on_stat_io_unsafe(store, msg: Stat) -> StatResult:
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
        return StatResult(task_def=task_def, result=result_ref)

    # Case 2: only def, FOUND
    if task_def is not None:
        logging.info('STAT: FOUND %s', msg.name)
        return StatResult(task_def=task_def, result=None)

    # Case 3: only children
    # This means a legacy store that was missing tasks definition
    # persistency, or a corruption. This was originally treated as DONE, but
    # in the current model there is no way to make the peer accept a missing
    # or fake definition
    # if children is not None:

    # Case 4: nothing
    logging.info('STAT: NOT FOUND %s', msg.name)
    return StatResult(None, None)
