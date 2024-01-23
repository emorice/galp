"""
ReplyValue serializers
"""

from functools import singledispatch

from galp.serializer import dump_model

from .types import ReplyValue, Put

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
