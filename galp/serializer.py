"""
Serialization utils
"""

from typing import Any, List, Callable, TypeVar
import logging

import msgpack
import dill

from pydantic import BaseModel, ValidationError, TypeAdapter

from galp.task_types import Task, StepType, TaskDef, TaskName

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

def dump_model(model: BaseModel, exclude: set[str] | None = None) -> bytes:
    """
    Serialize pydantic model with msgpack

    Args:
        model: the pydantic model to dump
        exclude: set of fields to exclude from the serialization
    """
    return msgpack.dumps(model.model_dump(exclude=exclude))

T = TypeVar('T', bound=BaseModel)

def load_model(model_type: type[T], payload: bytes, **extra_fields: Any
        ) -> T:
    """
    Load a msgpack-serialized pydantic model

    Also works on union of models (though the call won't type check).

    Args:
        model_type: The pydantic model class of the object to create
        payload: The msgpack-encoded buffer with the object data
        extra_fieds: attributes to add to the decoded payload before validation,
            intended to add out-of-band fields to the structure.

    Raises:
        DeserializeError on any failed deserialization or validation.
    """
    try:
        doc = msgpack.loads(payload)
        if not isinstance(doc, dict):
            raise TypeError
        doc.update(extra_fields)
    # Per msgpack docs:
    # "unpack may raise exception other than subclass of UnpackException.
    # If you want to catch all error, catch Exception instead.:
    except Exception as exc: # pylint: disable=broad-except
        err = 'Invalid msgpack message'
        logging.exception(err)
        raise DeserializeError(err) from exc

    try:
        return TypeAdapter(model_type).validate_python(doc)
    except ValidationError as exc:
        err = 'Invalid model data'
        logging.exception(err)
        raise DeserializeError(err) from exc


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
