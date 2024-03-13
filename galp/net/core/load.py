"""
Specific parsing logic for core layer
"""

from galp.result import Result, Ok
from galp.serialize import LoadError
from galp.serializer import load_model
from galp.net.base.load import LoaderDict, default_loader, UnionLoader
from galp.net.requests.load import Put, put_loader

from .types import Message, Reply, RequestId, ReplyValue, RemoteError

def _get_reply_value_type(request_id: RequestId) -> type[ReplyValue]:
    """
    Magic to extract type of reply value associated with request key

    Inherently fragile, but should break in fairly obvious ways
    """
    req_type = MessageLoader.get_type(request_id.verb)
    return req_type.reply_type # type: ignore

def _remote_error_loader(request_id: RequestId, frames: list[bytes]) -> Result[Reply, LoadError]:
    match frames:
        case [frame]:
            return load_model(str, frame).then(
                    lambda text: Ok(Reply(request_id, RemoteError(text)))
                    )
        case _:
            return LoadError('Wrong number of frames')

def _ok_value_loader(request_id: RequestId, frames: list[bytes]) -> Result[Reply, LoadError]:
    rep_type = _get_reply_value_type(request_id)
    if rep_type is Put:
        loaded = put_loader(frames)
    else:
        loaded = UnionLoader[rep_type].load(frames) # type: ignore
    return loaded.then(lambda value: Ok(Reply(request_id, Ok(value))))

def _reply_value_loader(request_id: RequestId, frames: list[bytes]) -> Result[Reply, LoadError]:
    match frames:
        case [ok_frame, *value_frames]:
            return load_model(bool, ok_frame).then(
                    lambda is_ok: (
                        _ok_value_loader(request_id, value_frames)
                        if is_ok else _remote_error_loader(request_id, value_frames)
                        )
                    )
        case _:
            return LoadError('Wrong number of frames')

def _reply_loader(frames: list[bytes]) -> Result[Reply, LoadError]:
    """Constructs a Reply"""
    match frames:
        case [core_frame, *value_frames]:
            return load_model(RequestId, core_frame).then(
                    lambda request_id: _reply_value_loader(request_id, value_frames)
                    )
        case _:
            return LoadError('Wrong number of frames')

_message_loaders = LoaderDict(default_loader)
_message_loaders[Reply] = _reply_loader

class MessageLoader(UnionLoader[Message]): # pylint: disable=too-few-public-methods
    """Loader for Message union"""
    loaders = _message_loaders

parse_core_message = MessageLoader.load
