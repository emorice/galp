"""
Caching utils
"""

import os
import json
import diskcache

from galp.graph import Handle

class CacheStack():
    """Asynchronous cache proxy

    if `dirpath` is none, serialized objects are only kept in memory and no
    persistence is done"""
    def __init__(self, dirpath, serializer):
        self.dirpath = dirpath

        self.nativecache = dict()
        self._handles = dict()

        if dirpath is None:
            self.serialcache = dict()
        else:
            self.serialcache = diskcache.Cache(dirpath)

        self.serializer = serializer


    async def contains(self, name):
        # Mind the short circuit
        return (name in self.nativecache) or (name in self.serialcache)

    async def get_native(self, handle):
        """
        Get a native object form the cache.

        Either finds it in memory, or deserialize from persistent storage.
        As a small optimization, in the latter case the native object is also
        stored in memory.
        """
        try:
            # Direct from memory
            return self.nativecache[handle.name]
        except KeyError:
            # Deserialize from cold storage
            serial = self.serialcache[handle.name]
            native = self.serializer.loads(handle, serial)
            # Hook into memory cache
            self.nativecache[handle.name] = native
            self._handles[handle.name] = handle
            return native

    async def get_serial(self, name):
        """
        Get a serialized object form the cache.

        For now we prioritize serializing each time from memory, but this could
        change.
        """
        try:
            # Serialize from memory
            native = self.nativecache[name]
            handle = self._handles[name]
            return self.serializer.dumps(handle, native)
        except KeyError:
            # Direct from persistent storage
            return self.serialcache[name]

    async def put_native(self, handle, obj):
        """Puts a native object in the cache.

        For now this also eagerly serialize it and commit it to persistent cache.
        """
        self.nativecache[handle.name] = obj
        self._handles[handle.name] = handle

        self.serialcache[handle.name] = self.serializer.dumps(handle, obj)

    async def put_serial(self, name, serial):
        """
        Simply pass the underlying object to the underlying cold cache.

        No serialization involved.
        """
        self.serialcache[name] = serial

