"""
Caching utils
"""

from typing import Any

import diskcache
import msgpack

from galp.task_types import (TaskName, TaskReference, Task, TaskDef,
        TaskNode, LiteralTaskDef)
from galp.graph import make_child_task_def
from galp.serializer import Serializer, serialize_child, load_task_def

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
    def __init__(self, dirpath, serializer: Serializer) -> None:
        self.dirpath = dirpath

        if dirpath is None:
            self.serialcache = {}
        else:
            self.serialcache = diskcache.Cache(dirpath, cull_limit=0)

        self.serializer = serializer


    def contains(self, name: TaskName) -> bool:
        """
        Whether the store contains a named resource

        Args:
            names: bytes, the resource name
        Returns:
            boolean
        """
        return name + b'.children' in self.serialcache

    def __contains__(self, name: TaskName) -> bool:
        return self.contains(name)

    def get_native(self, name: TaskName, shallow=False) -> Any:
        """
        Get a native object form the cache.

        Either finds it in memory, or deserialize from persistent storage.

        Args:
            name: the task name
            shallow: if False, the child tasks MUST have been checked in the
                cache first, and this method will extract them and inject them
                at the correct location in the returned object. If True, only
                the root object is returned with any linked task replaced by a
                TaskReference.
        """
        data, children = self.get_serial(name)

        if shallow:
            native_children = [ TaskReference(child_name)
                    for child_name in children ]
        else:
            native_children = [
                self.get_native(
                    child_name
                    )
                for child_name in children
                ]

        native = self.serializer.loads(data, native_children)
        return native

    def get_children(self, name: TaskName) -> list[TaskName]:
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

    def get_serial(self, name: TaskName) -> tuple[bytes, list[TaskName]]:
        """
        Get a serialized object form the cache.

        Returns:
            a tuple of one bytes object and one list of taskname (data, children).
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

    def put_native(self, name: TaskName, obj: Any, scatter: int | None = None) -> list[TaskName]:
        """Puts a native object in the cache.

        For now this eagerly serializes it and commits it to persistent cache.
        Recursive call if scatter is given. Returns the child tasks
        encountered when serializing (the true Task objects inside obj, not the
        "virtual" tasks of iterable handles)

        Args:
            obj: the native object to store

        Returns:
            A list containing the names of the Task objects encountered when
            serializing the object. The results of these tasks must be first
            added to the store for `get_native` to be able to construct the full
            object.
        """

        if scatter is not None:
            # Logical composite handle
            ## Recursively store the children
            child_names = []
            struct = []
            _len = 0
            for i, sub_obj in enumerate(obj):
                sub_ndef = make_child_task_def(name, i)
                child_names.append(sub_ndef.name)
                self.put_task_def(sub_ndef)
                # Recursive scatter is nor a thing yet, though we easily could
                self.put_native(sub_ndef.name, sub_obj, scatter=None)
                struct.append(serialize_child(i))
                _len += 1
            assert _len == scatter

            payload = msgpack.packb(struct)
            self.put_serial(name, (payload, child_names))
            return []

        data, children = self.serializer.dumps(obj)
        child_names = [ c.name for c in children ]
        self.put_serial(name, (data, child_names))

        # Recursively store the child task definitions
        for child in children:
            self.put_task(child)
        return child_names

    def put_serial(self, name: TaskName, serialized: tuple[bytes, list[TaskName]]):
        """
        Simply pass the underlying object to the underlying cold cache.

        No serialization involved, except for the children
        """
        data, children = serialized

        assert not isinstance(children, bytes) # guard against legacy code

        self.serialcache[name + b'.data'] = data
        self.serialcache[name + b'.children'] = msgpack.packb(children)

    def put_task_def(self, task_def: TaskDef) -> None:
        """
        Non-recursively store the dictionnary describing the task
        """
        key = task_def.name + b'.task'
        if key in self.serialcache:
            return

        self.serialcache[key] = msgpack.packb(task_def.model_dump())

    def put_task(self, task: Task) -> None:
        """
        Recursively store a task definition

        If the task is a literal, this also stores the included task result as a
        resource.
        """

        if not isinstance(task, TaskNode):
            # Task reference. The definition must have been recorded previously,
            # so there is nothing to do
            return

        self.put_task_def(task.task_def)

        for child in task.dependencies:
            self.put_task(child)

        if isinstance(task.task_def, LiteralTaskDef):
            self.put_native(task.name, task.data)

    def get_task_def(self, name: TaskName) -> TaskDef:
        """
        Loads a task definition, non-recursively
        """
        return load_task_def(name, self.serialcache[name + b'.task'])

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
