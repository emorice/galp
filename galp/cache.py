"""
Caching utils
"""

import diskcache

from galp.graph import SubTask, NonIterableHandleError
from galp.serializer import Serializer

class CacheStack():
    """Synchronous cache proxy.

    If `dirpath` is none, serialized objects are only kept in memory and no
    persistence is done.

    If a path is given, the store may hold some elements in memory. The current
    implementation does not, in order to priorize a low memory footprint.
    """
    def __init__(self, dirpath, serializer: Serializer):
        self.dirpath = dirpath

        if dirpath is None:
            self.serialcache = {}
        else:
            self.serialcache = diskcache.Cache(dirpath, cull_limit=0)

        self.serializer = serializer


    def contains(self, name):
        """
        Whether the store contains a named resource

        Args:
            names: bytes, the resource name
        Returns:
            boolean
        """
        return name + b'.proto' in self.serialcache

    def __contains__(self, name):
        return self.contains(name)

    def get_native(self, name):
        """
        Get a native object form the cache.

        Either finds it in memory, or deserialize from persistent storage.
        """
        proto, data, children = self.get_serial(name)
        native_children = [
            self.get_native(
                SubTask.gen_name(name, i)
                )
            for i in range(children)
            ]

        native = self.serializer.loads(proto, data, native_children)
        return native

    def get_serial(self, name):
        """
        Get a serialized object form the cache.

        Returns:
            a triplet of two bytes objects and one int (proto, data, children).
            If there is no data, None is returned instead.
        """
        try:
            data = self.serialcache[name + b'.data']
        except KeyError:
            data = None
        try:
            children = int.from_bytes(
                self.serialcache[name + b'.children'],
                'big')
        except KeyError:
            children = 0
        proto = self.serialcache[name + b'.proto']
        return (
            proto,
            data,
            children
            )

    def put_native(self, handle, obj):
        """Puts a native object in the cache.

        For now this eagerly serializes it and commits it to persistent cache.
        Recursive call if handle is iterable.
        """
        try:
            # Logical composite handle
            ## Recursively store the children
            for sub_handle, sub_obj in zip(handle, obj):
                self.put_native(sub_handle, sub_obj)

            self.put_serial(handle.name, (b'tuple', None, len(obj)))
        except NonIterableHandleError:
            proto, data = self.serializer.dumps(obj)
            self.put_serial(handle.name, (proto, data, 0))


    def put_serial(self, name, serialized):
        """
        Simply pass the underlying object to the underlying cold cache.

        No serialization involved, except for the children number.
        """
        proto, data, children = serialized

        assert isinstance(children, int)

        self.serialcache[name + b'.proto'] = proto
        self.serialcache[name + b'.data'] = data if data is not None else b''
        self.serialcache[name + b'.children'] = children.to_bytes(1, 'big')
