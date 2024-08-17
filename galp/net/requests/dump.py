"""
ReplyValue serializers
"""

from galp.result import Ok, Progress
from galp.serializer import dump_model
from galp.pack import dump

from .types import RemoteError

def dump_reply_value(value: Ok | Progress | RemoteError) -> list[bytes]:
    """
    Serializes a reply value.

    For Put, the data field is kept as-is a serialized frame.
    """
    match value:
        case Ok(ok_value):
            return [b'ok', *dump(ok_value)]
        case RemoteError() as err:
            return [b'error', dump_model(err.error)]
        case Progress() as prog:
            return [b'progress', dump_model(prog.status)]
