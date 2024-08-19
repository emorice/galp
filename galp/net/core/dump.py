"""
Core message serialization
"""

from typing import TypeVar, Callable, TypeAlias

from galp.result import Ok
from .types import Message, Reply, BaseRequest, RemoteError, Progress
from .load import dump_message, get_request_id, MessageTypeMap # pylint: disable=unused-import # reexport


M = TypeVar('M')

Writer: TypeAlias = Callable[[M], list[bytes]]
"""
Function that converts M to a list of bytes. This is defined as a type alias:
    * as a shortcut because Callable is verbose, and
    * as a central point to define what is the result type of the serialization
"""

V = TypeVar('V')

def add_request_id(write: Writer[Message], request: BaseRequest[V]
                   ) -> Writer[Ok[V] | Progress | RemoteError]:
    """Stack a Reply with given request id"""
    return lambda value: write(Reply(get_request_id(request), value))
