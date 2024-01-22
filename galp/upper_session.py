"""
Core-layer writer
"""

from dataclasses import dataclass
from functools import singledispatch

from galp.messages import Message, Reply, ReplyValue, Put
from galp.writer import TransportMessage, Writer, add_frames
from galp.serializer import dump_model

# ReplyValue serialization
# ------------------------

@singledispatch
def _dump_reply_value_data(value: ReplyValue) -> list[bytes]:
    return [dump_model(value)]

@_dump_reply_value_data.register
def _(value: Put) -> list[bytes]:
    return [dump_model(value.children), value.data]

def dump_reply_value(value: ReplyValue) -> list[bytes]:
    """
    Serializes a reply value.

    For Put, the data field is kept as-is a serialized frame.
    """
    return [value.message_get_key(), *_dump_reply_value_data(value)]

# Core message serialization
# --------------------------

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

# Core message session
# --------------------

@dataclass
class UpperSession:
    """
    Session encapsulating a galp message to be sent
    """
    write_lower: Writer

    def write(self, message: Message) -> TransportMessage:
        """
        Write a complete message from a galp message object.

        Route is specified through the lower_session attribute.
        """
        return add_frames(
                self.write_lower, dump_message(message)
                )([])
