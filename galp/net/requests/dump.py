"""
ReplyValue serializers
"""

from functools import singledispatch

from galp.result import Ok, Result
from galp.serializer import dump_model
from galp.task_types import FlatResultRef

from .types import ReplyValue, Put, RemoteError

@singledispatch
def _dump_ok_reply_value_data(ok_value) -> list[bytes]:
    return [ok_value.message_get_key(), dump_model(ok_value)]

@_dump_ok_reply_value_data.register
def _(value: Put) -> list[bytes]:
    return [dump_model(value.children), value.data]

@_dump_ok_reply_value_data.register
def _(value: FlatResultRef) -> list[bytes]:
    return [dump_model(value)]

def dump_reply_value(value: Result[ReplyValue, RemoteError]) -> list[bytes]:
    """
    Serializes a reply value.

    For Put, the data field is kept as-is a serialized frame.
    """
    match value:
        case Ok(ok_value):
            return [dump_model(True), *_dump_ok_reply_value_data(ok_value)]
        case RemoteError() as err:
            return [dump_model(False), dump_model(err.error)]
