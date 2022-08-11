"""
Serialization utils
"""

from typing import Any, Tuple, List

import dill

class DeserializeError(ValueError):
    """
    Exception raised to wrap any error encountered by the deserialization
    backends
    """
    def __init__(self):
        super().__init__('Failed to deserialize')

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
            # if any native children are present, assume a tuple, else a dill
            # type. Both will be superseded soon.
            if native_children:
                if data:
                    raise ValueError("Extra payload with tuple of resources")
                return tuple(native_children)
            return dill.loads(data)
        except Exception as exc:
            raise DeserializeError from exc

    def dumps(self, obj: Any) -> Tuple[bytes, bytes]:
        """
        Serialize the data.

        Note that this always uses the default serializer. This is because
        higher-level galp resources like lists of resources are normally
        serialized one by one, so the higher-level object is never directly
        passed down to the serializer.
        """
        return dill.dumps(obj)
