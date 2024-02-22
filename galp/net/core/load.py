"""
Specific parsing logic for core layer
"""

from galp.result import Result, Ok
from galp.serialize import LoadError
from galp.serializer import load_model
from galp.net.base.load import LoaderDict, default_loader, UnionLoader
from galp.net.requests.load import ReplyValueLoader

from .types import Message, Reply, RequestId, ReplyValue

def _get_reply_value_type(request: RequestId) -> type[ReplyValue]:
    """
    Magic to extract type of reply value associated with request key

    Inherently fragile, but should break in fairly obvious ways
    """
    req_type = MessageLoader.get_type(request.verb)
    return req_type.reply_type # type: ignore

def _reply_loader(frames: list[bytes]) -> Result[Reply, LoadError]:
    """
    Constructs a Reply
    """
    match frames:
        case [core_frame, *value_frames]:
            return load_model(RequestId, core_frame).then(
                    lambda request: (
                        ReplyValueLoader[_get_reply_value_type(request)] # type: ignore[misc]
                        .load(value_frames)
                        .then(lambda value: Ok(Reply(request, value)))
                        )
                    )
        case _:
            return LoadError('Wrong number of frames')

_message_loaders = LoaderDict(default_loader)
_message_loaders[Reply] = _reply_loader

class MessageLoader(UnionLoader[Message]): # pylint: disable=too-few-public-methods
    """Loader for Message union"""
    loaders = _message_loaders

parse_core_message = MessageLoader.load
