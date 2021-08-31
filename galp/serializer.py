"""
Serialization utils
"""

import json
import time
import logging

import dill
import numpy as np
import pyarrow as pa

from typing import Any, Tuple, List
from galp.typing import ArrayLike, Table

class Serializer:
    """
    Abstraction for a serialization strategy
    """

    def get_backend(self, handle, proto=None):
        if proto == b'tuple':
            return TupleSerializer

        return DillSerializer

    def loads(self, handle, proto: bytes, data: bytes, native_children: List[Any]) -> Any:
        """
        Unserialize the data in the payload, possibly using metadata from the
        handle.
        """
        return self.get_backend(handle, proto).loads(data, native_children)

    def dumps(self, handle, obj: Any) -> Tuple[bytes, bytes]:
        """
        Serialize the data.
        """
        backend = self.get_backend(handle)
        return backend.proto_id, backend.dumps(obj)

class TupleSerializer(Serializer):
    proto_id = b'tuple'

    @staticmethod
    def loads(payload, native_children):
        return tuple(native_children)

class JsonSerializer(Serializer):
    """Trivial wrapper around json to always return bytes"""
    proto_id = b'json'

    @staticmethod
    def loads(payload, _):
        return json.loads(payload)

    @staticmethod
    def dumps(obj):
        return json.dumps(obj).encode('ascii')

class DillSerializer(Serializer):
    """Trivial wrapper around dill"""
    proto_id = b'dill'

    @staticmethod
    def loads(payload, _):
        return dill.loads(payload)

    @staticmethod
    def dumps(obj):
        return dill.dumps(obj, byref=True)

class ArrowTensorSerializer(Serializer):
    """Uses Arrow ipc as a tensor serialization method.

    Note that arrow allows to do serialization-free sharing, which is planned to
    be integrated in the future.
    """
    proto_id = b'arrow.tensor'

    @staticmethod
    def dumps(obj):
        bos = pa.BufferOutputStream()

        # np.asarray should handle array-like types and not create too many
        # copies
        tensor = pa.Tensor.from_numpy(np.asarray(obj))
        pa.ipc.write_tensor(tensor, bos)

        return bos.getvalue().to_pybytes()

    @staticmethod
    def loads(buf, _):
        """
        Todo: handle errors more nicely since buffer is user input and could
        contain anything
        """

        reader = pa.BufferReader(buf)
        tensor = pa.ipc.read_tensor(reader)
        return tensor.to_numpy()

class ArrowTableSerializer(Serializer):
    """Uses Arrow ipc as a serialization method.

    Note that arrow allows to do serialization-free sharing, which is planned to
    be integrated in the future.
    """
    proto_id = b'arrow.table'

    @staticmethod
    def dumps(obj):
        bos = pa.BufferOutputStream()

        # Table-like
        writer = pa.ipc.new_file(bos, obj.schema)
        writer.write(obj)
        writer.close()

        return bos.getvalue().to_pybytes()

    @staticmethod
    def loads(buf, _):
        """
        Todo: handle errors more nicely since buffer is user input and could
        contain anything
        """

        reader = pa.ipc.open_file(buf)
        return reader.read_all()
