"""
Core message serialization
"""

from typing import TypeVar
from functools import singledispatch

from galp.result import Result
from galp.serializer import dump_model
from galp.net.requests.dump import dump_reply_value, dump_put
from galp.net.base.dump import Writer
from .types import (Message, Reply, get_request_id, BaseRequest, RemoteError,
    Upload)

@singledispatch
def _dump_message_data(message: Message) -> list[bytes]:
    return [dump_model(message)]

@_dump_message_data.register
def _(message: Reply) -> list[bytes]:
    return [dump_model(message.request), *dump_reply_value(message.value)]

@_dump_message_data.register
def _(message: Upload) -> list[bytes]:
    return [dump_model(message.task_def), *dump_put(message.payload)]

def dump_message(message: Message) -> list[bytes]:
    """
    Serialize a core message

    For replies, the enclosed value is dumped to separate extra frames.
    """
    return [message.message_get_key(), *_dump_message_data(message)]

V = TypeVar('V')

def add_request_id(write: Writer[Message], request: BaseRequest[V]
                   ) -> Writer[Result[V, RemoteError]]:
    """Stack a Reply with given request id"""
    return lambda value: write(Reply(get_request_id(request), value))
