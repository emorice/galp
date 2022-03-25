"""
General routines to build and operate on graphs of tasks.
"""
import json
import logging
import hashlib
import inspect
from itertools import chain

from typing import get_type_hints, get_args
from dataclasses import dataclass

from galp.eventnamespace import EventNamespace

class Step():
    """Object wrapping a function that can be called as a pipeline step

    'Step' refer to the function itself, 'Task' to the function with its
    arguments: 'add' is a step, 'add(3, 4)' a task. You can run a step twice,
    with different arguments, but that would be two different tasks, and a task
    is only ever run once.

    Args:
        items: int, signal this task eventually returns a collection of items
            objects, and allows to index or iterate over the task to generate
            handle pointing to the individual items.
    """
    def __init__(self, function, items=None):
        self.function = function
        self.key = bytes(function.__module__ + '::' + function.__qualname__, 'ascii')
        self.items = items

        # Compat with hint-style
        if items is None:
            hints = get_type_hints(self.function)
            if 'return' in hints:
                n_rets = len(get_args(hints['return']))
                if n_rets:
                    logging.warning(
                        "DEPRECATED: setting items from type hint (%s)",
                        function.__name__)
                    self.items = n_rets


    def make_handles(self, name, arg_names, kwarg_names):
        """
        Make initial handles.
        """
        handle = Handle(name, self.items)

        # arg handles are never iterable
        arg_handles = [
            Handle(arg_name, 0)
            for arg_name in arg_names
            ]

        kwarg_handles = {
            kw: Handle(kwname, 0)
            for kw, kwname in kwarg_names.items()
            }

        return handle, arg_handles, kwarg_handles

class Task():
    """
    Object wrapping a step invocation with its arguments

    Args:
        vtag: arbitrary object whose ascii() representation will be hashed and
            used in name generation, thus invalidating any previous names each
            time the vtag in the code is changed.
        rerun: (bool) True if a unique seed should be added at each loading and
          included as a vtag, causing unique names to be generated
          and the step to be rerun for each python client session.
    """
    def __init__(self, step, args, kwargs, rerun=False, vtag=None, items=None):
        self.step = step
        self.args = [self.ensure_task(arg) for arg in args]
        self.kwargs = { kw.encode('ascii'): self.ensure_task(arg) for kw, arg in kwargs.items() }
        self.vtags = (
            [ self.hash_one(ascii(vtag).encode('ascii')) ]
            if vtag is not None else [])
        self.name = self.gen_name(
            step.key,
            [ arg.name for arg in self.args ],
            { k : v.name for k, v in self.kwargs.items() },
            [ tag for tag in self.vtags ]
            )
        self.handle, self.arg_handles, self.kwarg_handles = self.step.make_handles(
            self.name,
            [ arg.name for arg in self.args ],
            { kw: arg.name for kw, arg in self.kwargs.items() }
            # note: for python 3.7+ this has the same order than kwargs
            )

        # Set the hereis upstream handles manually
        for task, handle in zip(
            chain(self.args, self.kwargs.values()),
            chain(self.arg_handles, self.kwarg_handles.values()),
            ):
            if hasattr(task, 'hereis'):
                task.handle = handle
            else:
                # todo: we have, as expected, both a downstream and upstream handle for
                # each task, they probably should be checked for compatibilty.
                pass

    @staticmethod
    def ensure_task(obj):
        """Wrap obj in a hereis Task if it does not seem to be a Task"""
        if hasattr(obj, 'name') and hasattr(obj, 'dependencies'):
            return obj
        return HereisTask(obj)

    @property
    def dependencies(self):
        """Shorthand for all the tasks this task directly depends on"""
        deps = list(self.args)
        deps.extend(self.kwargs.values())
        return deps

    @staticmethod
    def hash_one(payload):
        return hashlib.sha256(payload).digest()

    @staticmethod
    def _san(bts):
        """This constrains the range of values of the resulting bytes"""
        return bts.hex().encode('ascii')

    @staticmethod
    def gen_name(step_name, arg_names, kwarg_names, vtags):
        """Create a resource name.

        Args:
            step_name: bytes
            arg_names: list of bytes
            kwarg_names: bytes-keyed dict of bytes.
            vtags: list of bytes

        Returns:
            digest as bytes.

        We simply mash together the step name, the tags and the args name in order.
        The only difficulty is the order of keyword arguments, so we sort kwargs
        by keyword (in original byte representation).
        All input is sanitized by hexing it, then building a representation from
        it.

        """
        sortedkw = sorted(list(kwarg_names.items()))

        # Step name
        m = Task._san(step_name)
        # Version tags
        m += b'['
        m += b','.join(
            [ Task._san(n) for n in vtags]
            )
        m += b']'
        # Inputs
        m += b'('
        m += b','.join(
            [ Task._san(n) for n in arg_names]
            +
            [ Task._san(kw) + b'=' + Task._san(n) for kw, n in sortedkw ]
            )
        m += b')'
        logging.debug("Hashed value is %s:", m)

        return Task.hash_one(m)

    def __iter__(self):
        """
        If the function underlying the task is type-hinted as returning a tuple,
        returns a generator yielding sub-task objects allowing to refer to
        individual members of the result independently.

        """
        return (SubTask(self, sub_handle) for sub_handle in self.handle)

    def __getitem__(self, index):
        return SubTask(self, self.handle[index])

    @property
    def description(self):
        """Return a readable description of task"""
        return str(self.step.key, 'ascii')

class SubTask(Task):
    """
    A Task refering to an item of a Task representing a collection.

    Submission of a SubTask will cause the original Task to be submitted and
    executed. However, using a subtask as an argument or a Client.collect target
    will only cause one specific item to be serialized and sent over the
    network to the next step.
    """

    def __init__(self, parent, handle):
        self.parent = parent
        self.handle = handle
        self.name = handle.name

    @staticmethod
    def gen_name(parent_name, index_name):
        """
        Create a resource name.
        """

        m = Task._san(parent_name)
        m += b'['
        m += Task._san(index_name)
        m += b']'

        return Task.hash_one(m)

    @property
    def dependencies(self):
        return [self.parent]

    @property
    def description(self):
        """Return a readable description of task"""
        return self.parent.description + '[sub]'

@dataclass
class Handle():
    """Generic structure representing a resource, i.e. the result of the task,
    primarily identified by its name but also containing metadata
    """
    name: bytes
    items: int = 0

    @property
    def has_items(self):
        return bool(self.items)

    @property
    def n_items(self):
        if self.items:
            return self.items
        return 0

    def __iter__(self):
        if not self.has_items:
            raise NonIterableHandleError
        else:
            return (
                Handle(
                    SubTask.gen_name(self.name, str(i).encode('ascii')),
                    0
                    )
                for i in range(self.items)
                )

    def __getitem__(self, index):
        if not self.has_items:
            raise NonIterableHandleError
        return Handle(
                SubTask.gen_name(self.name, str(index).encode('ascii')),
                0)

class HereisTask(Task):
    """
    Task wrapper for data provided direclty as arguments in graph building.

    Args:
        obj: arbitrary object to wrap.
        type_hint: needed since there is no step to get the metadata from.
    """

    def __init__(self, obj):
        self.hereis = obj

        # Todo: more robust hashing, but this is enough for most case where
        # hereis resources are a good fit (more complex objects would tend to be
        # actual step outputs)
        self.name = self.hash_one(json.dumps(obj).encode('ascii'))

    @property
    def dependencies(self):
        return []

    @property
    def description(self):
        """Return a readable description of task"""
        return '[hereis]'

class StepSet(EventNamespace):
    """A collection of steps"""

    def step(self, *decorated, **options):
        """Decorator to make a function a step.

        This has two effects:
            * it makes the function a delayed function, so that calling it will
              return an object pointing to the actual function, not actually
              execute it. This object is the Task.
            * it register the function and information about it in a event
              namespace, so that it can be called from a key.

        For convenience, the decorator can be applied in two fashions:
        ```
        @step
        def foo():
            pass
        ```
        or
        ```
        @step(param=value, ...)
        def foo():
            pass
        ```
        This is allowed because the decorator checks whether it was applied
        directly to the function, or called with named arguments. In the second
        case, note that arguments must be given by name for the call to be
        unambiguous.

        See Task for the possible options.
        """
        def _step_maker(function):
            step = Step(
                function,
                items=options['items'] if 'items' in options else None
                )
            self.on(step.key)(step)
            def _task_maker(*args, **kwargs):
               return Task(step, args, kwargs, **options)
            return _task_maker

        if decorated:
            return _step_maker(*decorated)
        else:
            return _step_maker

    def __call__(self, *decorated, **options):
        """
        Shortcut for StepSet.step
        """
        return self.step(*decorated, **options)

    def get(self, key):
        """More memorable shortcut for handler"""
        return self.handler(key)

class NonIterableHandleError(TypeError):
    """
    Specific sub-exception raised when trying to iterate an atomic handle.
    """
    pass
