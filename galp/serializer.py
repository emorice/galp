"""
Serialization utils
"""

from typing import Any, List
import logging

import msgpack
import dill

TaskType = type('TaskType', tuple(), {})
StepType = type('StepType', tuple(), {})

class DeserializeError(ValueError):
    """
    Exception raised to wrap any error encountered by the deserialization
    backends
    """
    def __init__(self, msg=None):
        super().__init__(msg or 'Failed to deserialize')

CHILD_EXT_CODE = 0
_DILL_EXT_CODE = 1

def ext_hook(native_children):
    """
    ExtType to object for msgpack
    """
    def _hook(code, data):
        if code == CHILD_EXT_CODE:
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

def default(children):
    """
    Out-of-band sub-tasks and dill fallback

    Modifies children in-place
    """
    def _default(obj):
        if isinstance(obj, StepType):
            obj = obj()
        if isinstance(obj, TaskType):
            index = len(children)
            children.append(obj)
            return msgpack.ExtType(
                CHILD_EXT_CODE,
                index.to_bytes(4, 'little')
                )

        return msgpack.ExtType(
                _DILL_EXT_CODE,
                dill.dumps(obj)
                )
    return _default

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
            return msgpack.unpackb(data, ext_hook=ext_hook(native_children),
                    raw=False, use_list=False)
        except Exception as exc:
            raise DeserializeError from exc

    def dumps(self, obj) -> bytes:
        """
        Serialize the data.

        Note that this always uses the default serializer. This is because
        higher-level galp resources like lists of resources are normally
        serialized one by one, so the higher-level object is never directly
        passed down to the serializer.

        Args:
            obj: object to serialize
        """
        children = []
        # Modifies children in place
        payload = msgpack.packb(obj, default=default(children), use_bin_type=True)
        return payload, children
