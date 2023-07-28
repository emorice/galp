"""
General routines to build and operate on graphs of tasks.
"""
import hashlib
import inspect
import warnings
import functools

from typing import Any, Callable, TypeVar

import msgpack

from galp.task_types import (TaskName, StepType, TaskNode, TaskInput, Task,
        LiteralTaskDef, CoreTaskDef, NamedTaskDef, ChildTaskDef, QueryTaskDef,
        TaskOp, BaseTaskDef)
from galp.serializer import Serializer

_serializer = Serializer() # Actually stateless, safe

def hash_one(payload: bytes) -> bytes:
    """
    Hash argument with sha256
    """
    return hashlib.sha256(payload).digest()

def obj_to_name(canon_rep: Any) -> TaskName:
    """
    Generate a task name from a canonical representation of task made from basic
    types.

    (Current implememtation may not be canonical yet, this is an aspirational
    docstring)
    """
    payload = msgpack.packb(canon_rep)
    name = TaskName(hash_one(payload))
    return name

def ensure_task_node(obj: Any) -> TaskNode:
    """Makes object into a task in some way.

    If it's a step, try to call it to get a task.
    Else, wrap into a Literal task
    """
    if isinstance(obj, TaskNode):
        return obj
    if isinstance(obj, StepType):
        return obj()
    return make_literal_task(obj)

def ensure_task_input(obj: Any) -> tuple[TaskInput, Task]:
    """
    Makes object a task or a simple query

    Returns: a tuple (tin, node) where tin is a task input suitable for
        referencing the task while node is the full task object
    """
    if isinstance(obj, TaskNode) and isinstance(obj.task_def, QueryTaskDef):
        # Only allowed query for now, will be extended in the future
        dep, = obj.dependencies
        if obj.task_def.query in ['$base']:
            return (
                    TaskInput(op=TaskOp.BASE, name=obj.task_def.subject),
                    dep
                    )
    # Everything else is treated as task to be recursively run and loaded
    node = ensure_task_node(obj)
    return (
            TaskInput(op=TaskOp.SUB, name=node.name),
            node
            )

T = TypeVar('T', bound=BaseTaskDef)

def make_task_def(cls: type[T], attrs, extra=None) -> T:
    """
    Generate a name from attribute and extra and create a named task def of the
    wanted type
    """
    name = obj_to_name(msgpack.dumps((attrs, extra),
        default=lambda m: m.model_dump()))
    return cls(name=name, **attrs)

def make_literal_task(obj: Any) -> TaskNode:
    """
    Build a Literal TaskNode out of an arbitrary python object
    """
    # Todo: more robust hashing, but this is enough for most case where
    # literal resources are a good fit (more complex objects would tend to be
    # actual step outputs)
    obj_bytes, dependencies = _serializer.dumps(obj)

    tdef = {'children': [dep.name for dep in dependencies]}
    # Literals are an exception to naming: they are named by value, so the
    # name is *not* derived purely from the definition object

    return TaskNode(
            task_def=make_task_def(LiteralTaskDef, tdef, obj_bytes),
            dependencies=dependencies,
            data=obj,
            )

def make_core_task(step: 'Step', args: list[Any], kwargs: dict[str, Any],
                   vtag: str | None = None, items: int | None = None) -> TaskNode:
    """
    Build a core task from a function call
    """
    # Before anything else, type check the call. This ensures we don't wait
    # until actually trying to run the task to realize we're missing
    # arguments.
    try:
        full_kwargs = dict(kwargs, **{
            kw: WorkerSideInject()
            for kw in _WORKER_INJECTABLES
            if kw in step.kw_names
            })
        inspect.signature(step.function).bind(*args, **full_kwargs)
    except TypeError as exc:
        raise TypeError(
                f'{step.function.__name__}() {str(exc)}'
                ) from exc

    # Collect all arguments as nodes
    arg_inputs = []
    kwarg_inputs = {}
    nodes = []
    for arg in args:
        tin, node = ensure_task_input(arg)
        arg_inputs.append(tin)
        nodes.append(node)
    for key, arg in kwargs.items():
        tin, node = ensure_task_input(arg)
        kwarg_inputs[key] = tin
        nodes.append(node)

    tdef = dict(
        args=arg_inputs,
        kwargs=kwarg_inputs,
        step=step.key,
        vtags=([ascii(vtag) ] if vtag is not None else []),
        scatter=items
        )

    ndef = make_task_def(CoreTaskDef, tdef)

    return TaskNode(task_def=ndef, dependencies=nodes)

def make_child_task_def(parent: TaskName, index: int) -> NamedTaskDef:
    """
    Derive a Child NamedTaskDef from a given parent name

    This does not check whether the operation is legal
    """
    return make_task_def(ChildTaskDef, dict(parent=parent, index=index))

def make_child_task(parent: TaskNode, index: int) -> TaskNode:
    """
    Derive a Child TaskNode from a given task node

    This does not check whether the operation is legal
    """
    assert parent.task_def.scatter is not None
    assert index <= parent.task_def.scatter

    return TaskNode(
            task_def=make_child_task_def(parent.name, index),
            dependencies=[parent]
            )

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

    def __init__(self, scope: 'Block', function: Callable, is_view: bool = False, **task_options):
        self.function = function
        self.key = function.__module__ + '::' + function.__qualname__
        self.task_options = task_options
        self.scope = scope
        self.kw_names = {}
        self.has_var_kw = False
        self.is_view = is_view
        try:
            sig = inspect.signature(self.function)
            for name, param in sig.parameters.items():
                if param.kind == param.VAR_KEYWORD:
                    self.has_var_kw = True
                    continue
                if param.kind not in (
                    param.POSITIONAL_OR_KEYWORD,
                    param.KEYWORD_ONLY):
                    continue
                self.kw_names[name] = param.default != param.empty
        except ValueError:
            pass
        functools.update_wrapper(self, self.function)

    def __call__(self, *args, **kwargs) -> TaskNode:
        """
        Symbolically call the function wrapped by this step, returning a Task
        object representing the eventual result.

        Inject arguments from the scope if any are found.
        """
        bound_step = self._inject()
        injected_tasks = self._call_bound(bound_step, kwargs)
        all_kwargs = self.collect_kwargs(kwargs, injected_tasks)

        return self.make_task(args, all_kwargs)

    def make_task(self, args, kwargs) -> TaskNode:
        """
        Create actual Task object from the given args.

        Does not perform the injection
        """
        return make_core_task(self, args, kwargs, **self.task_options)

    def collect_kwargs(self, given, injected):
        """
        Collect dependencies, watching for duplicates

        If the step has variadic keyword arguments, all the given kwargs are
        passed, but only the injectable args explictly declared.
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
        if self.has_var_kw:
            _kwargs.update(given)

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
            # We still add it to the post_order as we'll need to inject it,
            # unless it's worker-side
            if not isinstance(injectable, StepType):
                oset.remove(name)
                cset.add(name)
                if not isinstance(injectable, WorkerSideInject):
                    post_order.append(name)
                pending.pop()
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

    def __get__(self, obj, objtype=None):
        """
        Normally, a function wrapper can be transparently applied to methods too
        through the descriptor protocol, but it's not clear what galp should do
        in that case, so we explicitly error out.

        The special function is defined anyway because code may rely on it to
        detect that the Step object is a function wrapper.
        """

        raise NotImplementedError('Cannot only wrap functions, not methods')

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

class NoSuchStep(ValueError):
    """
    No step with the given name has been registered
    """

# Placholder type for an object injected by the worker
WorkerSideInject = type('WorkerSideInject', tuple(), {})

# Keys reserved for resources injected by the worker at runtime.
_WORKER_INJECTABLES = ['_galp']

class Block:
    """A collection of steps and bound arguments"""
    def __init__(self, steps=None):
        # Note: if we inherit steps, we don't touch their scope
        self._steps = {} if steps is None else steps

        self._injectables = {
                k: WorkerSideInject()
                for k in _WORKER_INJECTABLES
                }

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

    def view(self, *decorated, **options):
        """
        Shortcut to register a view step
        """
        options['is_view'] = True
        return self.step(*decorated, **options)

    def __call__(self, *decorated, **options):
        """
        Shortcut for StepSet.step
        """
        return self.step(*decorated, **options)

    def get(self, key: str) -> Step:
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

class StepSet(Block):
    """
    Obsolete alias for PipelineBlock
    """
    def __init__(self, *args, **kwargs):
        warnings.warn('StepSet is now an alias to Block and will be removed in the'
                ' future', FutureWarning, stacklevel=2)
        super().__init__(*args, **kwargs)

def query(subject: Any, query: Any) -> TaskNode:
    """
    Build a Query task node
    """
    subj_node = ensure_task_node(subject)
    tdef = dict(query=query, subject=subj_node.name)
    return TaskNode(
            task_def=make_task_def(
                QueryTaskDef, tdef
                ),
            dependencies=[subj_node]
            )
