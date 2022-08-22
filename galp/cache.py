"""
Caching utils
"""

import diskcache
import msgpack

from galp.graph import NonIterableHandleError, TaskName
from galp.serializer import Serializer, CHILD_EXT_CODE

class StoreReadError(Exception):
    """
    Something, attached as the cause of this exception, prevented us from
    reading the result despite it seemingly being in the store (object not in
    store raises KeyError instead).
    """

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
        return name + b'.children' in self.serialcache

    def __contains__(self, name):
        return self.contains(name)

    def get_native(self, name):
        """
        Get a native object form the cache.

        Either finds it in memory, or deserialize from persistent storage.
        """
        data, children = self.get_serial(name)
        native_children = [
            self.get_native(
                child_name
                )
            for child_name in children
            ]

        native = self.serializer.loads(data, native_children)
        return native

    def get_children(self, name):
        """
        Gets the list of sub-resources of a resource in store
        """
        try:
            children = [
                TaskName(child_name)
                for child_name in msgpack.unpackb(
                    self.serialcache[name + b'.children']
                    )
                ]
        except KeyError:
            raise
        except Exception as exc:
            raise StoreReadError from exc

        return children

    def get_serial(self, name):
        """
        Get a serialized object form the cache.

        Returns:
            a tuple of one bytes object and one int (data, children).
            If there is no data, None is returned instead.
        """
        children = self.get_children(name)

        try:
            data = self.serialcache[name + b'.data']
        except KeyError:
            data = None
        return (
            data,
            children
            )

    def put_native(self, handle, obj):
        """Puts a native object in the cache.

        For now this eagerly serializes it and commits it to persistent cache.
        Recursive call if handle is iterable. Returns the child tasks
        encountered when serializing (the true Task objects inside obj, not the
        "virtual" tasks of iterable handles)
        """
        try:
            # Logical composite handle
            ## Recursively store the children
            children = []
            struct = []
            for i, (sub_handle, sub_obj) in enumerate(zip(handle, obj)):
                children.append(sub_handle.name)
                self.put_native(sub_handle, sub_obj)
                struct.append(
                    msgpack.ExtType(
                        code=CHILD_EXT_CODE,
                        data=i.to_bytes(4, 'little')
                        )
                    )
            payload = msgpack.packb(struct)
            self.put_serial(handle.name, (payload, children))
            return []
        except NonIterableHandleError:
            pass

        data, children = self.serializer.dumps(obj)
        child_names = [ c.name for c in children ]
        self.put_serial(handle.name, (data, child_names))

        # Recursively store the child task definitions
        for child in children:
            self.put_task(child)
        return child_names

    def put_serial(self, name, serialized):
        """
        Simply pass the underlying object to the underlying cold cache.

        No serialization involved, except for the children
        """
        data, children = serialized

        assert not isinstance(children, bytes) # guard against legacy code

        self.serialcache[name + b'.data'] = data
        self.serialcache[name + b'.children'] = msgpack.packb(children)

    def put_task(self, task):
        """
        Recursively store a task definition

        If the task is a literal, this also stores the included task result as a
        resource.
        """
        key = task.name + b'.task'
        if key in self.serialcache:
            return

        self.serialcache[key] = msgpack.packb(task.to_dict())

        for child in task.dependencies:
            self.put_task(child)

        if hasattr(task, 'literal'):
            self.put_native(task.handle, task.literal)

    def get_task(self, name):
        """
        Returns a task definition, non-recursively
        """
        task_dict = msgpack.unpackb(
            self.serialcache[name + b'.task']
            )
        task_dict['name'] = name
        return task_dict

def add_store_argument(parser, optional=False):
    """
    Fills argparse ArgumentParser with either optional or mandatory store
    location
    """
    help_msg = 'directory where the steps results will be stored'
    if optional:
        parser.add_argument('-s', '--store', help=help_msg)
    else:
        parser.add_argument('store', help=help_msg)
