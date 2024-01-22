"""
Core-layer writer
"""

from dataclasses import dataclass
from functools import singledispatch

from galp.messages import Message, Reply, ReplyValue, Put
from galp.writer import TransportMessage, Writer, add_frames
from galp.serializer import dump_model

@singledispatch
def dump_reply_value_data(value: ReplyValue) -> list[bytes]:
    """
    Serializes the reply value without key type
    """
    return [dump_model(value)]

@dump_reply_value_data.register
def _(value: Put) -> list[bytes]:
    """
    Serializes a put by keeping the data buffer as a frame
    """
    return [dump_model(value.children), value.data]

def dump_reply_value(value: ReplyValue) -> list[bytes]:
    """
    Serializes a reply value along its type key
    """
    return [value.message_get_key(), *dump_reply_value_data(value)]

@singledispatch
def dump_message_data(message: Message) -> list[bytes]:
    """
    Dump a message, without the type identification frame

    Fallback is to use pydantic to serialize the class, assuming it's a
    dataclass

    For replies, the value is handled as a separate frame
    """
    return [dump_model(message)]

@dump_message_data.register
def _(message: Reply) -> list[bytes]:
    return [dump_model(message.request), *dump_reply_value(message.value)]

def dump_message(message: Message) -> list[bytes]:
    """
    Serialize a message
    """
    return [message.message_get_key(), *dump_message_data(message)]

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
