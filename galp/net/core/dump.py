"""
Core message serialization
"""

from typing import TypeVar
from functools import singledispatch

from galp.result import Ok, Progress
from galp.net.requests.dump import dump_reply_value
from galp.net.base.dump import Writer
from galp.pack import dump
from .types import Message, Reply, get_request_id, BaseRequest, RemoteError

@singledispatch
def _dump_message_data(message: Message) -> list[bytes]:
    return dump(message)

@_dump_message_data.register
def _(message: Reply) -> list[bytes]:
    return [*dump(message.request), *dump_reply_value(message.value)]

def dump_message(message: Message) -> list[bytes]:
    """
    Serialize a core message

    For replies, the enclosed value is dumped to separate extra frames.
    """
    return [message.message_get_key(), *_dump_message_data(message)]

V = TypeVar('V')

def add_request_id(write: Writer[Message], request: BaseRequest[V]
                   ) -> Writer[Ok[V] | Progress | RemoteError]:
    """Stack a Reply with given request id"""
    return lambda value: write(Reply(get_request_id(request), value))
