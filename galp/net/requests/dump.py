"""
ReplyValue serializers
"""

from functools import singledispatch

from galp.result import Ok
from galp.serializer import dump_model
from galp.task_types import FlatResultRef, Serialized

from .types import ReplyValue, RemoteError, Progress

@singledispatch
def _dump_ok_reply_value_data(ok_value) -> list[bytes]:
    return [ok_value.message_get_key(), dump_model(ok_value)]

def dump_serialized(obj: Serialized) -> list[bytes]:
    """
    Special serialization for Put, the data is out of band
    """
    return [dump_model(obj.children), obj.data]

@_dump_ok_reply_value_data.register
def _(value: Serialized) -> list[bytes]:
    return dump_serialized(value)

@_dump_ok_reply_value_data.register
def _(value: FlatResultRef) -> list[bytes]:
    return [dump_model(value)]

def dump_reply_value(value: Ok[ReplyValue] | Progress | RemoteError) -> list[bytes]:
    """
    Serializes a reply value.

    For Put, the data field is kept as-is a serialized frame.
    """
    match value:
        case Ok(ok_value):
            return [b'ok', *_dump_ok_reply_value_data(ok_value)]
        case RemoteError() as err:
            return [b'error', dump_model(err.error)]
        case Progress() as prog:
            return [b'progress', dump_model(prog.status)]
