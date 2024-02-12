"""
Specific parsing logic for core layer
"""

from typing import TypeVar
from galp.result import Result, Ok
from galp.serialize import LoadError
from galp.serializer import load_model
from galp.net.base.load import (LoaderDict, default_loader, parse_message_type,
        Loader)
from galp.net.requests.load import load_reply_value

from .types import Message, RequestId, StatReply, GetReply, SubmitReply

T = TypeVar('T', StatReply, GetReply, SubmitReply)

def make_reply_loader(reply_type: type[T]) -> Loader[T]:
    """Constructs a Reply"""
    def _reply_loader(frames: list[bytes]) -> Result[T, LoadError]:
        match frames:
            case [core_frame, *value_frames]:
                return load_model(RequestId, core_frame).then(
                        lambda request: load_reply_value(value_frames).then(
                            lambda value: Ok(reply_type(request, value))
                            )
                        )
            case _:
                return LoadError('Wrong number of frames')
    return _reply_loader

_message_loaders = LoaderDict(default_loader)
_message_loaders[GetReply] = make_reply_loader(GetReply)
_message_loaders[StatReply] = make_reply_loader(StatReply)
_message_loaders[SubmitReply] = make_reply_loader(SubmitReply)

parse_core_message = parse_message_type(Message, _message_loaders)
