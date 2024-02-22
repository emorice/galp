"""
Specific parsing logic for core layer
"""

from galp.result import Result, Ok
from galp.serialize import LoadError
from galp.serializer import load_model
from galp.net.base.load import LoaderDict, default_loader, parse_message_type
from galp.net.requests.load import load_reply_value

from .types import Message, Reply, RequestId, ReplyValue

def _get_reply_value_type(request: RequestId) -> type[ReplyValue]:
    """
    Magic to extract type of reply value associated with request key

    Inherently fragile, but should break in fairly obvious ways
    """
    req_type = Message.message_get_type(request.verb)
    assert req_type
    return req_type.__orig_bases__[0].__args__[0] # type: ignore[attr-defined]

def _reply_loader(frames: list[bytes]) -> Result[Reply, LoadError]:
    """
    Constructs a Reply
    """
    match frames:
        case [core_frame, *value_frames]:
            return load_model(RequestId, core_frame).then(
                    lambda request: load_reply_value(
                        _get_reply_value_type(request), value_frames
                        ).then(
                        lambda value: Ok(Reply(request, value))
                        )
                    )
        case _:
            return LoadError('Wrong number of frames')

_message_loaders = LoaderDict(default_loader)
_message_loaders[Reply] = _reply_loader

parse_core_message = parse_message_type(Message, _message_loaders)
