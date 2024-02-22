"""
Specific logic to parsing request-layer objects
"""

from typing import TypeVar

from galp.result import Result, Ok
from galp.task_types import TaskRef, TaskSerializer
from galp.serialize import LoadError
from galp.serializer import load_model
from galp.net.base.load import LoaderDict, default_loader, UnionLoader
from .types import Put, GetReplyValue, StatReplyValue, SubmitReplyValue

def _put_loader(frames: list[bytes]) -> Result[Put, LoadError]:
    """Loads a Put with data frame"""
    match frames:
        case [core_frame, data_frame]:
            return load_model(list[TaskRef], core_frame).then(
                    lambda children: Ok(Put(children=children, data=data_frame,
                                            _loads=TaskSerializer.loads))
                    )
        case _:
            return LoadError('Wrong number of frames')

_value_loaders = LoaderDict(default_loader)
_value_loaders[Put] = _put_loader

T = TypeVar('T', GetReplyValue, StatReplyValue, SubmitReplyValue)

class ReplyValueLoader(UnionLoader[T]): # pylint: disable=too-few-public-methods
    """Loader for value types, standard union loader with special case for Put"""
    loaders = _value_loaders
