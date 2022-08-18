"""
General routines to build and operate on graphs of tasks.
"""
import hashlib
import inspect

from dataclasses import dataclass

import msgpack

from galp.serializer import Serializer, TaskType, StepType

class TaskName(bytes):
    """
    Simpler wrapper around bytes with a shortened, more readable repr
    """
    def __repr__(self):
        return self.__str__()

    def __str__(self):
        _hex = self.hex()
        return _hex[:6] + '..' + _hex[-2:]

_serializer = Serializer() # Actually stateless, safe

def hash_one(payload):
    """
    Hash argument with sha256
    """
    return hashlib.sha256(payload).digest()

def obj_to_name(canon_rep):

    """
    Generate a task name from a canonical representation of task made from basic
    types
    """
    payload = msgpack.packb(canon_rep)
    name = TaskName(hash_one(payload))
    return name

def ensure_task(obj):
    """Makes object into a task in some way.

    If it's a step, try to call it to get a task.
    Else, wrap into a Literal task
    """
    if isinstance(obj, TaskType):
        return obj
    if isinstance(obj, Step):
        return obj()
    return LiteralTask(obj)

class Step(StepType):
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

    def __init__(self, scope, function, **task_options):
        self.function = function
        self.key = bytes(function.__module__ + '::' + function.__qualname__, 'ascii')
        self.task_options = task_options
        self.scope = scope
        self.kw_names = []
        try:
            sig = inspect.signature(self.function)
            for name, param in sig.parameters.items():
                if param.kind not in (
                    param.POSITIONAL_OR_KEYWORD,
                    param.KEYWORD_ONLY):
                    continue
                self.kw_names.append(name)
        except ValueError:
            pass

    def __call__(self, *args, **kwargs):
        """
        Symbolically call the function wrapped by this step, returning a Task
        object representing the eventual result.

        Inject arguments from the scope if any are found.
        """
        for name in self.kw_names:
            injectable = self.scope._injectables.get(name)
            if injectable:
                if name in kwargs:
                    raise ValueError(f'Duplicate argument {name}, '
                        'given as an argument but also injectable')
                if isinstance(injectable, WorkerSideInject):
                    # We raise error on naming conflicts, but fall short of
                    # actually injecting the object
                    continue
                kwargs[name] = injectable

        return Task(self, args, kwargs, **self.task_options)

    def make_handle(self, name):
        """
        Make initial handles.
        """
        items = self.task_options.get('items')
        return Handle(name, items=items)

class Task(TaskType):
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
    def __init__(self, step, args, kwargs, vtag=None, items=None):
        self.step = step
        self.args = [ensure_task(arg) for arg in args]
        self.kwargs = { kw.encode('ascii'): ensure_task(arg) for kw, arg in kwargs.items() }
        self.vtags = (
            [ ascii(vtag) ]
            if vtag is not None else [])
        self.name = self.gen_name(dict(
            step_name=step.key,
            arg_names=[ arg.name for arg in self.args ],
            kwarg_names={ k : v.name for k, v in self.kwargs.items() },
            vtags=self.vtags
            ))
        self.handle = Handle(self.name, items)

    @property
    def dependencies(self):
        """Shorthand for all the tasks this task directly depends on"""
        deps = list(self.args)
        deps.extend(self.kwargs.values())
        return deps

    @classmethod
    def gen_name(cls, task_dict):
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
        by keyword (in original byte representation). We also sort the vtags, so
        that they are seen as a set.
        """

        canon_rep = [
            cls.__name__,
            task_dict['step_name'], # Step name
            sorted(task_dict['vtags']), # Set of tags
            task_dict['arg_names'], # List of pos args
            sorted(list(task_dict['kwarg_names'].items())) # Map of kw args
            ]

        return obj_to_name(canon_rep)

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

class SubTask(TaskType):
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

    @classmethod
    def gen_name(cls, parent_name, index):
        """
        Create a resource name.
        """

        rep = [ cls.__name__, parent_name, str(index) ]

        return obj_to_name(rep)

    @property
    def dependencies(self):
        """
        List of tasks this task depends on.

        In this case this case it is the actual task we are derived from
        """
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
        """
        Whether the resource is known to be iterable
        """
        return bool(self.items)

    @property
    def n_items(self):
        """
        How many sub-resources this resource consists of, if known, else 0.
        """
        if self.items:
            return self.items
        return 0

    def __iter__(self):
        if not self.has_items:
            raise NonIterableHandleError
        return (
            Handle(
                SubTask.gen_name(self.name, i),
                0
                )
            for i in range(self.items)
            )

    def __getitem__(self, index):
        if not self.has_items:
            raise NonIterableHandleError
        return Handle(
                SubTask.gen_name(self.name, index),
                0)

class LiteralTask(TaskType):
    """
    Task wrapper for data provided direclty as arguments in graph building.

    Args:
        obj: arbitrary object to wrap.
        type_hint: needed since there is no step to get the metadata from.
    """

    def __init__(self, obj):
        self.literal = obj

        # Todo: more robust hashing, but this is enough for most case where
        # literal resources are a good fit (more complex objects would tend to be
        # actual step outputs)
        self.data, self.dependencies = _serializer.dumps(obj, child_objects=True)

        rep = [
            self.__class__.__name__, self.data,
            [ dep.name for dep in self.dependencies ]
            ]

        self.name = obj_to_name(rep)
        self.handle = Handle(self.name)

    @property
    def description(self):
        """Return a readable description of task"""
        return '[literal]'

class NoSuchStep(ValueError):
    """
    No step with the given name has been registered
    """

# Placholder type for an object injected by the worker
WorkerSideInject = type('WorkerSideInject', tuple(), {})

class StepSet:
    """A collection of steps"""
    def __init__(self, steps=None):
        # Note: if we inherit steps, we don't touch their scope
        self._steps = {} if steps is None else steps

        self._injectables = {
                k: WorkerSideInject()
                for k in self._worker_injectables
                }

    # Keys reserved for resources injected by the worker at runtime.
    _worker_injectables = ['_galp']

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
                self,
                function,
                **options,
                )
            self._injectables[function.__name__] = step
            self._steps[step.key] = step
            return step

        if decorated:
            return _step_maker(*decorated)
        return _step_maker

    def __call__(self, *decorated, **options):
        """
        Shortcut for StepSet.step
        """
        return self.step(*decorated, **options)

    def get(self, key):
        """Return step by full name"""
        try:
            return self._steps[key]
        except KeyError:
            raise NoSuchStep(key) from None

    def bind(self, **kwargs):
        """
        Add the given objects to the injectables dictionnary of the scope
        """
        for key, value in kwargs.items():
            if  key in self._injectables:
                raise ValueError(f'Duplicate injection of "{key}"')
            self._injectables[key] = value

    @property
    def all_steps(self):
        """
        Dictionary of current steps
        """
        return self._steps

    def __contains__(self, key):
        return key in self._steps

    def __getitem__(self, key):
        return self.get(key)

    def __iadd__(self, other):
        self._steps.update(other._steps)
        # We do not merge the injectables by default
        return self

class NonIterableHandleError(TypeError):
    """
    Specific sub-exception raised when trying to iterate an atomic handle.
    """
