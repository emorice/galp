"""
Core message serialization
"""

from functools import singledispatch

from galp.serializer import dump_model
from galp.net.requests.dump import dump_reply_value
from .types import Message, Reply

@singledispatch
def _dump_message_data(message: Message) -> list[bytes]:
    return [dump_model(message)]

@_dump_message_data.register
def _(message: Reply) -> list[bytes]:
    return [dump_model(message.request), *dump_reply_value(message.value)]

def dump_message(message: Message) -> list[bytes]:
    """
    Serialize a core message

    For replies, the enclosed value is dumped to separate extra frames.
    """
    return [message.message_get_key(), *_dump_message_data(message)]
