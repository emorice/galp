"""
Specific parsing logic for core layer
"""

from galp.result import Ok, Progress
from galp.serialize import LoadError
from galp.net.base.load import LoaderDict, UnionLoader
from galp.pack import load

from .types import Message, Reply, RequestId, RemoteError

def _get_reply_value_type(request_id: RequestId) -> type:
    """
    Magic to extract type of reply value associated with request key

    Inherently fragile, but should break in fairly obvious ways
    """
    req_type = MessageLoader.get_type(request_id.verb)
    return req_type.reply_type # type: ignore

def _remote_error_loader(ok_frame: bytes, request_id: RequestId, frames: list[bytes]
                         ) -> Ok[Reply] | LoadError:
    if ok_frame not in (b'error', b'progress'):
        return LoadError('Bad value type')
    return load(str, frames).then(
            lambda text: Ok(Reply(
                request_id,
                RemoteError(text) if ok_frame == b'error' else Progress(text)
                ))
            )

def _ok_value_loader(request_id: RequestId, frames: list[bytes]) -> Ok[Reply] | LoadError:
    rep_type = _get_reply_value_type(request_id)
    loaded: Ok | LoadError = load(rep_type, frames)
    return loaded.then(lambda value: Ok(Reply(request_id, Ok(value))))

def _reply_value_loader(request_id: RequestId, frames: list[bytes]) -> Ok[Reply] | LoadError:
    match frames:
        case [ok_frame, *value_frames]:
            return (
                    _ok_value_loader(request_id, value_frames)
                    if ok_frame == b'ok'
                    else _remote_error_loader(ok_frame, request_id, value_frames)
                    )
        case _:
            return LoadError('Wrong number of frames')

def _reply_loader(frames: list[bytes]) -> Ok[Reply] | LoadError:
    """Constructs a Reply"""
    match frames:
        case [core_frame, *value_frames]:
            return load(RequestId, [core_frame]).then(
                    lambda request_id: _reply_value_loader(request_id, value_frames)
                    )
        case _:
            return LoadError('Wrong number of frames')

_message_loaders = LoaderDict(lambda cls: (lambda frames: load(cls, frames)))
_message_loaders[Reply] = _reply_loader

class MessageLoader(UnionLoader[Message]): # pylint: disable=too-few-public-methods
    """Loader for Message union"""
    loaders = _message_loaders

parse_core_message = MessageLoader.load
