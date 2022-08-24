"""
General routines to build and operate on graphs of tasks.
"""
import hashlib
import inspect

from dataclasses import dataclass

import msgpack

import galp
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
    if isinstance(obj, StepType):
        return obj()
    return LiteralTask(obj)

def task_deps(task_dict):
    """
    Gather the list of dependencies names from a task dictionnary
    """
    # Step-style Task, the dependencies are the tasks given as arguments
    if 'arg_names' in task_dict:
        return [ TaskName(dep) for dep in [
            *task_dict['arg_names'],
            *task_dict['kwarg_names'].values()
            ]]
    # SubTask, the dependency is the parent task
    if 'parent' in task_dict:
        return [ task_dict['parent'] ]

    # Literal, the dependencies are the tasks found embedded when walking the
    # object
    if 'children' in task_dict:
        return task_dict['children']

    raise NotImplementedError('task_deps', task_dict)

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
        self.kw_names = {}
        try:
            sig = inspect.signature(self.function)
            for name, param in sig.parameters.items():
                if param.kind not in (
                    param.POSITIONAL_OR_KEYWORD,
                    param.KEYWORD_ONLY):
                    continue
                self.kw_names[name] = param.default != param.empty
        except ValueError:
            pass

    def __call__(self, *args, **kwargs):
        """
        Symbolically call the function wrapped by this step, returning a Task
        object representing the eventual result.

        Inject arguments from the scope if any are found.
        """
        bound_step = self._inject()
        injected_tasks = self._call_bound(bound_step, kwargs)
        all_kwargs = self.collect_kwargs(kwargs, injected_tasks)
        return self.make_task(args, all_kwargs)

    def make_task(self, args, kwargs):
        """
        Create actual Task object from the given args.

        Does not perform the injection
        """
        return Task(self, args, kwargs, **self.task_options)

    def make_handle(self, name):
        """
        Make initial handles.
        """
        items = self.task_options.get('items')
        return Handle(name, items=items)

    def collect_kwargs(self, given, injected):
        """
        Collect dependencies, watching for duplicates
        """
        _kwargs = {}
        for arg_name in self.kw_names:
            if arg_name in given:
                if arg_name in injected:
                    raise TypeError(
                        f"Got multiple values for argument '{arg_name}' of "
                        f"step '{self}', once as an injection and once "
                        "as a keyword argument"
                        )
                _kwargs[arg_name] = given[arg_name]
            if arg_name in injected:
                _kwargs[arg_name] = injected[arg_name]
        return _kwargs

    def _inject(self):
        """
        Inject a step and then its injected dependencies, repetitively, until
        everything injectable in the step's scope has been found.

        At this point, the step is made into a bound step, that can still accept
        arguments to the original step, but also keyword arguments that fulfill a
        missing injection.

        This does not modify any global state and can be called safely again after
        registering new steps in the scope.
        """
        # Make a copy at call point
        injectables = dict(self.scope._injectables)

        # Closed set: steps already recursively visited and added to post_order
        cset = set()
        # Open set: steps visited but with children yet to be visited
        oset = set()

        first_injectables = [
                name
                for name in self.kw_names
                if name in injectables
                ]
        pending = list(first_injectables)

        post_order = []

        free_parameters = set()
        wanted_by = {}

        while pending:
            # Get a name that still needs to be injected, but leave
            # it on the stack
            name = pending[-1]

            # If closed already, nothing left to do
            if name in cset:
                pending.pop()
                continue

            # If open, it's the second visit, we close the node
            if name in oset:
                oset.remove(name)
                cset.add(name)
                post_order.append(name)
                pending.pop()
                continue

            # Else, we visit it
            # Mark as visited but still open
            oset.add(name)

            # Get the bound object
            injectable = injectables[name]

            # If not itself a Step, injection stops here
            # We still add it to the post_order we'll need to inject it,
            # unless it's worker-side
            if not isinstance(injectable, StepType):
                pending.pop()
                if not isinstance(injectable, WorkerSideInject):
                    post_order.append(name)
                continue

            # Push its unseen arguments on the stack
            for dep_name, has_default in injectable.kw_names.items():
                # Already fully processed, skip
                if dep_name in cset:
                    continue

                # If in open set, we have a cycle
                if dep_name in oset:
                    raise TypeError(f'Cyclic dependency of {name} on {dep_name}')

                # New name, keep the reason why it was pushed for better reporting
                wanted_by[dep_name] = name

                # Not injectable, add to the unbound parameters and skip
                if dep_name not in injectables:
                    if not has_default:
                        free_parameters.add(dep_name)
                    continue

                # Else, push it
                pending.append(dep_name)

        # At this point, we've explored the whole injectable graph, and we have a
        # post-order on the steps
        return {
            'post_order': post_order,
            'free_parameters': free_parameters,
            'wanted_by': wanted_by,
            'injectables': injectables,
            }

    def _call_bound(self, bound_step, kwargs):
        """
        Create a Task graph from a bound (injected) step and provided arguments
        """
        wanted_by = bound_step['wanted_by']
        injectables = bound_step['injectables']

        # Catch missing free parameters early
        for free in bound_step['free_parameters']:
            if free not in kwargs:
                chain = [free]
                while chain[-1] in wanted_by:
                    chain.append(wanted_by[chain[-1]])
                raise TypeError(
                    "No value given for argument: '"
                    + "',\n\trequired by '".join(chain)
                    + f"',\n\trequired by '{self.key.decode('ascii')}'"
                    )

        tasks = {}
        # Create tasks for all injected steps
        for name in bound_step['post_order']:
            injectable = injectables[name]
            if isinstance(injectable, StepType):
                _kwargs = injectable.collect_kwargs(kwargs, tasks)
                # Create actual task
                tasks[name] = injectable.make_task([], _kwargs)
            else:
                # This can be a Task or a literal
                # FIXME: we can't inject through a literal for now
                tasks[name] = injectable
        return tasks

    def __getitem__(self, index):
        """
        Return a new step representing the eventual result of injecting then
        subscripting this step
        """
        return SubStep(self, index)

class SubStep(StepType):
    """
    Step representing the indexation of the result of an other step
    """
    def __init__(self, parent, index):
        self.parent = parent
        self.index = index

    def __call__(self, *args, **kwargs):
        """
        Inject and call the parent step, then index the resulting task
        """
        return self.parent(*args, **kwargs)[self.index]

    @property
    def kw_names(self):
        """
        Injectable arguments, inherited from the step being indexed
        """
        return self.parent.kw_names

    def collect_kwargs(self, given, injected):
        """
        See Step
        """
        return self.parent.collect_kwargs(given, injected)

    def make_task(self, args, kwargs):
        """
        Create parent task and subscript it.
        """
        return self.parent.make_task(args, kwargs)[self.index]

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
        self.name = self.gen_name(self.to_dict())
        self.handle = Handle(self.name, items)

    def to_dict(self, name=False):
        """
        Returns a dictionnary representation of the task.
        """
        task_dict = {
            'step_name': self.step.key,
            'vtags': self.vtags,
            'arg_names': [ arg.name for arg in self.args ],
            'kwarg_names': { kw: kwarg.name for kw, kwarg in self.kwargs.items() }
            }
        if name:
            task_dict['name'] = self.name
        return task_dict

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
        if self.handle.has_items:
            # Soft getitem, the referenced task is a scatter type
            return SubTask(self, self.handle[index])
        # Hard getitem, we actually insert an extra task
        return galp.steps.getitem(self, index)

    def __str__(self):
        return f'{self.name} {str(self.step.key, "ascii")}'

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

    def to_dict(self, name=False):
        """
        Returns a dictionnary representation of the task.
        """
        task_dict = {
            'parent': self.parent.name
            }
        if name:
            task_dict['name'] = self.name
        return task_dict

    def __str__(self):
        return '{self.name} [sub] {self.parent}'

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
        self.data, self.dependencies = _serializer.dumps(obj)

        rep = [
            self.__class__.__name__, self.data,
            [ dep.name for dep in self.dependencies ]
            ]

        self.name = obj_to_name(rep)
        self.handle = Handle(self.name)

    def to_dict(self, name=False):
        """
        Dictionary representation of task
        """
        task_dict = {
            'children': [ dep.name for dep in self.dependencies ]
            }
        if name:
            task_dict['name'] = self.name
        return task_dict

    def __str__(self):
        return f'{self.name} [literal] {self.literal}'

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
