"""
Caching utils
"""

from typing import Any, Sequence

import diskcache # type: ignore[import] # Issue 85
import msgpack # type: ignore[import] # Issue 85

import galp.task_types as gtt
from galp.result import Ok
from galp.task_types import (TaskName, TaskRef, Task, TaskDef,
        TaskNode, TaskSerializer, Serialized, LiteralTaskNode)
from galp.serializer import serialize_child, dump_model, load_model, LoadError

class StoreReadError(Exception):
    """
    Something, attached as the cause of this exception, prevented us from
    reading the result despite it seemingly being in the store (object not in
    store raises KeyError instead).
    """

class Store():
    """Synchronous cache proxy.

    If `dirpath` is none, serialized objects are only kept in memory and no
    persistence is done.

    If a path is given, the store may hold some elements in memory. The current
    implementation does not, in order to priorize a low memory footprint.
    """
    def __init__(self, dirpath, serializer: type[TaskSerializer]) -> None:
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

    def get_native(self, name: TaskName, shallow=False) -> object:
        """
        Get a native object form the cache.

        Either finds it in memory, or deserialize from persistent storage.

        Args:
            name: the task name
            shallow: if False, the child tasks MUST have been checked in the
                cache first, and this method will extract them and inject them
                at the correct location in the returned object. If True, only
                the root object is returned with any linked task replaced by a
                TaskRef.
        """
        serialized = self.get_serial(name)

        if shallow:
            native_children: Sequence[object] = serialized.children
        else:
            native_children = [self.get_native(child.name)
                    for child in serialized.children]

        match serialized.deserialize(native_children):
            case LoadError() as err:
                raise StoreReadError(err)
            case Ok(native):
                return native
        assert False # type check bug ?

    def get_children(self, name: TaskName) -> gtt.FlatResultRef:
        """
        Gets the list of sub-resources of a resource in store

        Since the only way to get them there is `put_serial` that requires task
        refs, it is safe to get them out as task refs too ; meaning that the
        corresponding task definition is guaranteed to be in the store as well.
        """
        try:
            children = tuple(
                gtt.TaskRef(gtt.TaskName(child_name))
                for child_name in msgpack.unpackb(
                    self.serialcache[name + b'.children']
                    )
                )
        except KeyError:
            raise
        except Exception as exc:
            raise StoreReadError from exc

        return gtt.FlatResultRef(name, children)

    def get_serial(self, name: TaskName) -> Serialized:
        """
        Get a serialized object form the cache.

        Returns:
            a tuple of a bytes object, a list of taskname (data, children) and a
            callable usable to deserialize.
            If there is no data, None is returned instead as the first tuple item
        """
        children_ref = self.get_children(name)

        try:
            data = self.serialcache[name + b'.data']
        except KeyError:
            data = None
        return Serialized(data, children_ref.children, self.serializer.loads)

    def put_native(self, name: TaskName, obj: Any, scatter: int | None = None
                   ) -> gtt.FlatResultRef:
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
            child_refs = []
            struct = []
            _len = 0
            for i, sub_obj in enumerate(obj):
                sub_ndef = gtt.make_child_task_def(name, i)
                child_refs.append(self.put_child_task_def(sub_ndef))
                # Recursive scatter is nor a thing yet, though we easily could
                self.put_native(sub_ndef.name, sub_obj, scatter=None)
                struct.append(serialize_child(i))
                _len += 1
            assert _len == scatter

            payload = msgpack.packb(struct)
            self.put_serial(name, Serialized(payload, child_refs,
                                             TaskSerializer.loads))
            return gtt.FlatResultRef(name, children=tuple())

        serialized = self.serializer.dumps(obj, self.put_task)

        return self.put_serial(name, serialized)

    def put_serial(self, name: TaskName, serialized: Serialized
                   ) -> gtt.FlatResultRef:
        """
        Simply pass the underlying object to the underlying cold cache.

        This inherently low-level and unsafe, this is only meant to transfer
        objects effictiently between caches sharing a serializer.
        """
        self.serialcache[name + b'.data'] = serialized.data
        self.serialcache[name + b'.children'] = msgpack.packb(
                [c.name for c in serialized.children]
                )

        return gtt.FlatResultRef(name, tuple(serialized.children))

    def put_task_def(self, task_def: TaskDef) -> None:
        """
        Non-recursively store the dictionnary describing the task
        """
        key = task_def.name + b'.task'
        if key in self.serialcache:
            return

        self.serialcache[key] = dump_model(task_def)

    def put_child_task_def(self, task_def: gtt.ChildTaskDef) -> TaskRef:
        """
        Put a child task def.

        Since these never themselves have children (as opposed to core or
        literal tasks, e.g., this is enough to generate a reference to the child
        task
        """
        self.put_task_def(task_def)
        return gtt.TaskRef(task_def.name)

    def put_task(self, task: Task) -> TaskRef:
        """
        Recursively store a task definition, and returns a reference that
        represent a saved task definition. This should normally be the only way
        to create such a TaskRef.

        If the task is a literal, this also stores the included task result as a
        resource.
        """

        if not isinstance(task, TaskNode):
            # Task reference. The definition must have been recorded previously,
            # so there is nothing to do
            return task

        self.put_task_def(task.task_def)

        for child in task.dependencies:
            self.put_task(child)

        if isinstance(task, LiteralTaskNode):
            self.put_serial(task.name, task.serialized)

        return TaskRef(task.name)

    def get_task_def(self, name: TaskName) -> TaskDef:
        """
        Loads a task definition, non-recursively
        """
        # Load with a union, I don't know any way to type this
        task_def: Ok[TaskDef] | LoadError = \
                load_model(TaskDef, self.serialcache[name + b'.task']) # type: ignore[arg-type]
        match task_def:
            case LoadError() as err:
                raise StoreReadError(err)
            case Ok(tdef):
                return tdef

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
