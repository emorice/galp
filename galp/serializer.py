"""
Serialization utils
"""

from typing import Any, List
import logging

import msgpack
import dill

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

def default(obj):
    """
    Object to dill ExtType for msgpack
    """
    return msgpack.ExtType(
            _DILL_EXT_CODE,
            dill.dumps(obj)
            )

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

    def dumps(self, obj: Any) -> bytes:
        """
        Serialize the data.

        Note that this always uses the default serializer. This is because
        higher-level galp resources like lists of resources are normally
        serialized one by one, so the higher-level object is never directly
        passed down to the serializer.
        """
        return msgpack.packb(obj, default=default, use_bin_type=True)
