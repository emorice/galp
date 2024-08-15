"""
Specific logic to parsing request-layer objects
"""

from galp.result import Ok
from galp.task_types import TaskRef, Serialized
from galp.serialize import LoadError
from galp.serializer import load_model

def load_serialized(frames: list[bytes]) -> Ok[Serialized] | LoadError:
    """Loads a Serialized with data frame"""
    match frames:
        case [core_frame, data_frame]:
            return load_model(tuple[TaskRef, ...], core_frame).then(
                    lambda children: Ok(
                        Serialized(children=children, data=data_frame)
                        )
                    )
        case _:
            return LoadError('Wrong number of frames')
