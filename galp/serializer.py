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

    def get_backend(self, proto=None):
        """
        Returns the deserializer to use based on the given protocol.
        """
        if proto == b'tuple':
            return TupleSerializer

        return DillSerializer

    def loads(self, proto: bytes, data: bytes, native_children: List[Any]) -> Any:
        """
        Unserialize the data in the payload, possibly using metadata from the
        handle.

        Re-raises DeserializeError on any exception.
        """
        try:
            return self.get_backend(proto).loads(data, native_children)
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
        backend = self.get_backend()
        return backend.proto_id, backend.dumps(obj)

class BackendSerializer:
    """
    Common interface of concrete serializers
    """
    @staticmethod
    def loads(payload, native_children):
        """
        Takes a payload and already deserialized sub-resources, and returns a
        native object
        """
        raise NotImplementedError

    @staticmethod
    def dumps(obj):
        """
        Takes an object and returns a serialized payload
        """
        raise NotImplementedError

class TupleSerializer(BackendSerializer):
    """
    Serializer for high-level tuples of resources
    """
    proto_id = b'tuple'

    @staticmethod
    def loads(payload, native_children):
        if payload:
            raise ValueError("Extra payload with tuple of resources")
        return tuple(native_children)

    @staticmethod
    def dumps(obj):
        raise NotImplementedError

class DillSerializer(BackendSerializer):
    """Trivial wrapper around dill"""
    proto_id = b'dill'

    @staticmethod
    def loads(payload, native_children):
        return dill.loads(payload)

    @staticmethod
    def dumps(obj):
        return dill.dumps(obj, byref=True)
