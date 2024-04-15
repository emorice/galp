"""
Specific parsing logic for core layer
"""

from galp.result import Result, Ok
from galp.serialize import LoadError
from galp.serializer import load_model
from galp.net.base.load import LoaderDict, default_loader, UnionLoader
from galp.net.requests.load import Serialized, load_serialized
from galp.task_types import FlatResultRef, TaskDef

from .types import Message, Reply, RequestId, RemoteError, Upload

def _get_reply_value_type(request_id: RequestId) -> type:
    """
    Magic to extract type of reply value associated with request key

    Inherently fragile, but should break in fairly obvious ways
    """
    req_type = MessageLoader.get_type(request_id.verb)
    return req_type.reply_type # type: ignore

def _remote_error_loader(request_id: RequestId, frames: list[bytes]) -> Ok[Reply] | LoadError:
    match frames:
        case [frame]:
            return load_model(str, frame).then(
                    lambda text: Ok(Reply(request_id, RemoteError(text)))
                    )
        case _:
            return LoadError('Wrong number of frames')

def _ok_value_loader(request_id: RequestId, frames: list[bytes]) -> Ok[Reply] | LoadError:
    rep_type = _get_reply_value_type(request_id)
    loaded: Result
    if rep_type is Serialized:
        loaded = load_serialized(frames)
    elif rep_type is FlatResultRef:
        match frames:
            case [frame]:
                loaded = load_model(FlatResultRef, frame)
            case _:
                loaded = LoadError('Wrong number of frames')
    else:
        loaded = UnionLoader[rep_type].load(frames) # type: ignore
    return loaded.then(lambda value: Ok(Reply(request_id, Ok(value))))

def _reply_value_loader(request_id: RequestId, frames: list[bytes]) -> Ok[Reply] | LoadError:
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

def _reply_loader(frames: list[bytes]) -> Ok[Reply] | LoadError:
    """Constructs a Reply"""
    match frames:
        case [core_frame, *value_frames]:
            return load_model(RequestId, core_frame).then(
                    lambda request_id: _reply_value_loader(request_id, value_frames)
                    )
        case _:
            return LoadError('Wrong number of frames')

def _load_upload(frames: list[bytes]) -> Ok[Upload] | LoadError:
    match frames:
        case [task_def_frame, *ser_frames]:
            return load_model(TaskDef, task_def_frame).then( # type: ignore[arg-type]
                    lambda task_def: load_serialized(ser_frames).then(
                        lambda ser: Ok(Upload(task_def, ser))
                        )
                    )
        case _:
            return LoadError('Wrong number of frames')

_message_loaders = LoaderDict(default_loader)
_message_loaders[Reply] = _reply_loader
_message_loaders[Upload] = _load_upload

class MessageLoader(UnionLoader[Message]): # pylint: disable=too-few-public-methods
    """Loader for Message union"""
    loaders = _message_loaders

parse_core_message = MessageLoader.load
