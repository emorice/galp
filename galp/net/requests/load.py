"""
Specific logic to parsing request-layer objects
"""

from galp.result import Result, Ok
from galp.task_types import TaskRef, TaskSerializer
from galp.serialize import LoadError
from galp.serializer import load_model
from .types import Put

def put_loader(frames: list[bytes]) -> Result[Put, LoadError]:
    """Loads a Put with data frame"""
    match frames:
        case [core_frame, data_frame]:
            return load_model(list[TaskRef], core_frame).then(
                    lambda children: Ok(Put(children=children, data=data_frame,
                                            _loads=TaskSerializer.loads))
                    )
        case _:
            return LoadError('Wrong number of frames')
