"""
Specific logic to parsing request-layer objects
"""

from galp.task_types import TaskRef, TaskSerializer
from galp.serialize import LoadError
from galp.serializer import load_model
from galp.net.base.load import LoaderDict, default_loader, parse_message_type
from .types import ReplyValue, Put

def _put_loader(frames: list[bytes]) -> Put | LoadError:
    """Loads a Put with data frame"""
    match frames:
        case [core_frame, data_frame]:
            children = load_model(list[TaskRef], core_frame)
            if isinstance(children, LoadError):
                return children
            return Put(children=children, data=data_frame,
                    _loads=TaskSerializer.loads)
        case _:
            return LoadError('Wrong number of frames')

_value_loaders = LoaderDict(default_loader)
_value_loaders[Put] = _put_loader

load_reply_value = parse_message_type(ReplyValue, _value_loaders)
