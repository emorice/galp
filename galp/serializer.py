"""
Serialization utils
"""

import json
import time

import numpy as np
import pyarrow as pa

from typing import Any
from galp.typing import ArrayLike

class Serializer:
    """
    Abstraction for a serialization strategy
    """

    def get_backend(self, handle):
        if handle.type_hint is ArrayLike:
            return ArrowSerializer
        return JsonSerializer

    def loads(self, handle, payload: bytes) -> Any:
        """
        Unserialize the data in the payload, possibly using metadata from the
        handle.
        """
        return self.get_backend(handle).loads(payload)

    def dumps(self, handle, obj: Any) -> bytes:
        """
        Unserialize the data in the payload, possibly using metadata from the
        handle.
        """
        return self.get_backend(handle).dumps(obj)

class JsonSerializer(Serializer):
    """Trivial wrapper around json to always return bytes"""
    @staticmethod
    def loads(payload):
        return json.loads(payload)

    @staticmethod
    def dumps(obj):
        return json.dumps(obj).encode('ascii')

class ArrowSerializer(Serializer):
    """Uses Arrow ipc as a serialization method.

    Note that arrow allows to do serialization-free sharing, which is planned to
    be integrated in the future.
    """

    @staticmethod
    def dumps(obj):
        bos = pa.BufferOutputStream()

        tensor = pa.Tensor.from_numpy(obj)

        pa.ipc.write_tensor(tensor, bos)

        return bos.getvalue().to_pybytes()

    @staticmethod
    def loads(buf):
        """
        Todo: handle errors more nicely since buffer is user input and could
        contain anything
        """

        reader = pa.BufferReader(buf)
        tensor = pa.ipc.read_tensor(reader)
        return tensor.to_numpy()

