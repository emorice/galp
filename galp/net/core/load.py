"""
Specific parsing logic for core layer
"""

from galp.result import Result, Ok
from galp.serialize import LoadError
from galp.serializer import load_model
from galp.net.base.load import LoaderDict, default_loader, parse_message_type
from galp.net.requests.load import load_reply_value

from .types import Message, Reply, RequestId

def _reply_loader(frames: list[bytes]) -> Result[Reply, LoadError]:
    """
    Constructs a Reply
    """
    match frames:
        case [core_frame, *value_frames]:
            return load_model(RequestId, core_frame).then(
                    lambda request: load_reply_value(value_frames).then(
                        lambda value: Ok(Reply(request, value))
                        )
                    )
        case _:
            return LoadError('Wrong number of frames')

_message_loaders = LoaderDict(default_loader)
_message_loaders[Reply] = _reply_loader

parse_core_message = parse_message_type(Message, _message_loaders)
