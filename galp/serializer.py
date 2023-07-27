"""
Serialization utils
"""

from typing import Any, List, Callable
import logging

import msgpack
import dill

from galp.task_types import (Task, StepType, NamedTaskDef, TaskName, TaskDef,
                             CoreTaskDef, NamedCoreTaskDef, GNamedTaskDef,
                             is_core)

class DeserializeError(ValueError):
    """
    Exception raised to wrap any error encountered by the deserialization
    backends
    """
    def __init__(self, msg=None):
        super().__init__(msg or 'Failed to deserialize')

class Serializer:
    """
    Abstraction for a serialization strategy
    """

    def loads(self, data: bytes, native_children: List[Any]) -> Any:
        """
        Unserialize the data in the payload, possibly using metadata from the
        handle.

        Re-raises DeserializeError on any exception.
        """
        try:
            return msgpack.unpackb(data, ext_hook=_ext_hook(native_children),
                    raw=False, use_list=False)
        except Exception as exc:
            raise DeserializeError from exc

    def dumps(self, obj: Any) -> tuple[bytes, list[Task]]:
        """
        Serialize the data.

        Note that this always uses the default serializer. This is because
        higher-level galp resources like lists of resources are normally
        serialized one by one, so the higher-level object is never directly
        passed down to the serializer.

        Args:
            obj: object to serialize
        """
        children : list[Task] = []
        # Modifies children in place
        payload = msgpack.packb(obj, default=_default(children), use_bin_type=True)
        return payload, children

def serialize_child(index):
    """
    Serialized representation of a reference to the index-th child task of an
    object.

    This is also used when storing scattered tasks.
    """
    return msgpack.ExtType(
        _CHILD_EXT_CODE,
        index.to_bytes(4, 'little')
        )

def load_task_def(name: bytes, def_buffer: bytes,
                  task_def_t = TaskDef) -> NamedTaskDef:
    """
    Util to deserialize a named task def with the name and def split

    This is use both in cache and protocol, so it makes more sense to keep it
    here despite it not being related to the rest of the serialization code for
    now.

    Args:
        name: name of the task
        def_buffer: msgpack encoded task def
        task_def_t: additional type constraint on type of task def
    """
    return GNamedTaskDef[task_def_t].model_validate({
        'name': name,
        'task_def': msgpack.unpackb(
                    def_buffer
                    )
            })

def load_core_task_def(name: bytes, def_buffer: bytes) -> NamedCoreTaskDef:
    """
    Variant of load_task_def statically contrained to a CoreTaskDef
    """
    ndef = load_task_def(name, def_buffer, CoreTaskDef)
    assert is_core(ndef) # hint
    return ndef

def dump_task_def(named_def: NamedTaskDef) -> tuple[TaskName, bytes]:
    """
    Converse of load_task_def
    """
    return named_def.name, msgpack.dumps(named_def.task_def.model_dump())


# Private serializer helpers
# ==========================
# This should not be exposed to the serializer consumers

_CHILD_EXT_CODE = 0
_DILL_EXT_CODE = 1

def _default(children: list[Task]) -> Callable:
    """
    Out-of-band sub-tasks and dill fallback

    Modifies children in-place
    """
    def _children_default(obj):
        if isinstance(obj, StepType):
            obj = obj()
        if isinstance(obj, Task):
            index = len(children)
            children.append(obj)
            return msgpack.ExtType(
                _CHILD_EXT_CODE,
                index.to_bytes(4, 'little')
                )

        return msgpack.ExtType(
                _DILL_EXT_CODE,
                dill.dumps(obj)
                )
    return _children_default

def _ext_hook(native_children):
    """
    ExtType to object for msgpack
    """
    def _hook(code, data):
        if code == _CHILD_EXT_CODE:
            if not len(data) == 4:
                raise DeserializeError(f'Invalid child index {data}')
            index = int.from_bytes(data, 'little')
            try:
                return native_children[index]
            except (KeyError, IndexError) as exc:
                raise DeserializeError(f'Missing child index {data}') from exc
        if code == _DILL_EXT_CODE:
            logging.warning('Unsafe deserialization with dill')
            return dill.loads(data)
        raise DeserializeError(f'Unknown ExtType {code}')
    return _hook
