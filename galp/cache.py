"""
Caching utils
"""

import os
import json
import diskcache

from galp.graph import Handle

class CacheStack():
    """Asynchronous cache proxy"""
    def __init__(self, dirpath, serializer):
        self.dirpath = dirpath
        self.memcache = dict()
        self.diskcache = diskcache.Cache(dirpath)
        self.serializer = serializer

    async def contains(self, name):
        # Mind the short circuit
        return (name in self.memcache) or (name in self.diskcache)

    async def get(self, handle):
        """
        Get a native object form the cache.

        A handle is required instead of just the resource name since the handle
        may contain required metadata to deserialize the resource.
        """
        try:
            return self.memcache[handle.name]
        except KeyError:
            obj = self.serializer.loads(handle, self.diskcache[name])
            self.memcache[handle.name] = obj
            return obj

    async def put(self, handle, obj):
        """Puts a native object in the cache.

        Since this may serialize the object, resource metadata is necessary and
        a handle is used instead of just the identifier (name).
        """
        self.memcache[handle.name] = obj
        self.diskcache[handle.name] = self.serializer.dumps(handle, obj)

