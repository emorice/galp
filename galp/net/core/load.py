"""
Specific parsing logic for core layer
"""

from galp.result import Ok
from galp.pack import get_loader, set_loader, LoadException
from galp.pack import get_dumper, set_dumper, TypeMap, dump_part

from .types import Message, Reply, RequestId, RemoteError, Progress
from . import types as gm

def _get_reply_value_type(request_id: RequestId) -> type:
    """
    Magic to extract type of reply value associated with request key

    Inherently fragile, but should break in fairly obvious ways
    """
    req_type = MessageTypeMap.types[request_id.verb]
    return req_type.reply_type # type: ignore

def get_request_id(req: gm.BaseRequest) -> RequestId:
    """
    Make request id from Request
    """
    # Code calling get_request_id often want to use the generic, so we can't
    # simultaneously constrain the union
    key, _dumper = MessageTypeMap.get_key(req) # type:ignore[arg-type]
    return RequestId(key, req.input_id)

load_request_id = get_loader(RequestId)
load_remote_error = get_loader(str)
load_progress = get_loader(str)

def _reply_loader(doc, frames: list[bytes]) -> tuple[Reply, list[bytes]]:
    """Constructs a Reply"""
    req_id_doc, key, value_doc = doc
    request_id, frames = load_request_id(req_id_doc, frames)
    value: Ok | RemoteError | Progress
    if key == b'ok':
        rep_type = _get_reply_value_type(request_id)
        inner_value: object
        inner_value, extras = get_loader(rep_type)(value_doc, frames)
        value = Ok(inner_value)
    elif key == b'error':
        error, extras = load_remote_error(value_doc, frames)
        value = RemoteError(error)
    elif key == b'progress':
        status, extras = load_progress(value_doc, frames)
        value = Progress(status)
    else:
        raise LoadException('Bad type key for Reply')
    return Reply(request_id, value), extras

set_loader(Reply, _reply_loader)

# We also specify dumper here instead of dump.py as both need to be defined
# before we can instantiate the type map
dump_request_id = get_dumper(RequestId)

def _dump_reply(message: Reply) -> tuple[object, list[bytes]]:
    req_doc, extras = dump_request_id(message.request)
    match message.value:
        case Ok(ok_value):
            key, (value_doc, value_extras) = b'ok', dump_part(ok_value)
        case RemoteError() as err:
            key, (value_doc, value_extras) = b'error', dump_part(err.error)
        case Progress() as prog:
            key, (value_doc, value_extras) = b'progress', dump_part(prog.status)
    extras.extend(value_extras)
    return (req_doc, key, value_doc), extras

set_dumper(Reply, _dump_reply)

MessageTypeMap: TypeMap[bytes, Message] = TypeMap({
        b'exited': gm.Exited,
        b'fork': gm.Fork,
        b'ready': gm.Ready,
        b'pool_ready': gm.PoolReady,
        b'get': gm.Get,
        b'stat': gm.Stat,
        b'submit': gm.Submit,
        b'upload': gm.Upload,
        b'exec': gm.Exec,
        b'reply': gm.Reply,
        b'next_request': gm.NextRequest,
        })
dump_message = MessageTypeMap.dump
parse_core_message = MessageTypeMap.load
