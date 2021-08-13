"""
Caching utils
"""

import os
import json
import logging
import diskcache

from galp.graph import Handle, NonIterableHandleError, SubTask

class CacheStack():
    """Synchronous cache proxy

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


    def contains(self, name):
        # Mind the short circuit
        return (name in self.nativecache) or (name + b'.proto' in self.serialcache)

    def get_native(self, handle):
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
            proto, data, b_children = self.get_serial(handle.name)
            children = int.from_bytes(b_children, 'big')
            native_children = [
                self.get_native(
                    handle[i]
                    )
                for i in range(children)
                ]

            native = self.serializer.loads(handle, proto, data, native_children)
            # Hook into memory cache
            self.nativecache[handle.name] = native
            self._handles[handle.name] = handle
            return native

    def get_serial(self, name):
        """
        Get a serialized object form the cache.

        For now we prioritize serializing each time from memory, but this could
        change.

        Returns:
            a triplet of bytes objects (proto, data, children). Note that
            children is an int but still encoded as bytes at this stage.
            If there is no data, None is returned instead.
        """
        try:
            # Serialize from memory
            native = self.nativecache[name]
            handle = self._handles[name]
            return self.serializer.dumps(handle, native) + (int(0).to_bytes(1, 'big'),)
        except KeyError:
            # Direct from persistent storage
            try:
                data = self.serialcache[name + b'.data']
            except KeyError:
                data = None
            try:
                children = self.serialcache[name + b'.children']
            except KeyError:
                children = int(0).to_bytes(1, 'big')
            proto = self.serialcache[name + b'.proto']
            return (
                proto,
                data,
                children
                )
                

    def put_native(self, handle, obj):
        """Puts a native object in the cache.

        For now this also eagerly serialize it and commit it to persistent cache.
        Recursive call if handle is iterable.
        """
        try:
            # Logical composite handle
            ## Recursively store the children
            for sub_handle, sub_obj in zip(handle, obj):
                self.put_native(sub_handle, sub_obj)

            self.put_serial(handle.name, b'tuple', None, len(obj).to_bytes(1, 'big'))

        except NonIterableHandleError:
            self.nativecache[handle.name] = obj
            self._handles[handle.name] = handle

            proto, data = self.serializer.dumps(handle, obj)
            self.put_serial(handle.name, proto, data, int(0).to_bytes(1, 'big'))


    def put_serial(self, name, proto, data, children):
        """
        Simply pass the underlying object to the underlying cold cache.

        No serialization involved.
        """
        assert type(children) == bytes
        self.serialcache[name + b'.proto'] = proto
        self.serialcache[name + b'.data'] = data if data is not None else b''
        self.serialcache[name + b'.children'] = children

