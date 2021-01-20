"""
Serialization utils
"""

import json
import time

import numpy as np
import tables

from typing import Any
from galp.typing import ArrayLike

class Serializer:
    """
    Abstraction for a serialization strategy
    """

    def get_backend(self, handle):
        if handle.type_hint is ArrayLike:
            return HDFSerializer
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

class HDFSerializer(Serializer):
    """Uses HDF in-memory file images as a serialization method.

    See https://www.pytables.org/cookbook/inmemory_hdf5_files.html for details"""

    @staticmethod
    def dumps(obj):
        mem_file = tables.open_file(
            str(id(obj)), "a",
            driver="H5FD_CORE",
            driver_core_backing_store=0)

        mem_file.create_array('/', "object", obj)

        image = mem_file.get_file_image()

        mem_file.close()

        return image

    @staticmethod
    def loads(image):
        """
        Todo: handle errors more nicely since image is user input and could
        contain anything
        """
        ts = time.time()
        mem_file = tables.open_file(str(ts),
            driver="H5FD_CORE",
            driver_core_image=image,
            driver_core_backing_store=0)

        obj = mem_file.root.object

        mem_file.close()
