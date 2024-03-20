"""
Specific logic to parsing request-layer objects
"""

from galp.result import Result, Ok
from galp.task_types import TaskRef, TaskSerializer, Serialized
from galp.serialize import LoadError
from galp.serializer import load_model

def load_serialized(frames: list[bytes]) -> Result[Serialized, LoadError]:
    """Loads a Serialized with data frame"""
    match frames:
        case [core_frame, data_frame]:
            return load_model(list[TaskRef], core_frame).then(
                    lambda children: Ok(Serialized(children=children, data=data_frame,
                                            _loads=TaskSerializer.loads))
                    )
        case _:
            return LoadError('Wrong number of frames')
