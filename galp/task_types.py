"""
Abstract task types definitions
"""

import inspect
import functools
import logging
import hashlib
from importlib import import_module
from typing import Any, TypeAlias, TypeVar, Iterable, Callable, Sequence
from dataclasses import dataclass

import msgpack # type: ignore[import] # Issue #85

from galp.result import Ok
from galp.task_defs import (TaskName, LiteralTaskDef, TaskInput,
                            Resources, TaskOp, BaseTaskDef, CoreTaskDef,
                            ChildTaskDef, TaskDef, QueryDef)
from galp.default_resources import get_resources
from galp.serializer import Serializer, GenSerialized
from galp.pack import LoadError

# Core task type definitions
# ==========================

# Tasks: possibly recursive objects representing graphs of tasks
# --------------------------------------------------------------

@dataclass(frozen=True)
class TaskRef:
    """
    A reference to a defined task by name.

    This is intended to be created when a task definition is committed to
    storage, so that a task reference exists if and only if a task definition
    has been stored successfully.

    This contract is recursive, in the sense that a TaskRef should be
    issued only when all the inputs of a task have themselves been registered.
    """
    name: TaskName

    def tree_print(self, prefix: str = '', seen=None) -> str:
        """
        Debug-friendly printing
        """
        _ = seen
        return f'{prefix}Ref {self.name}\n'

    def __getitem__(self, index):
        # Hard getitem, we actually insert an extra task
        return getitem(self, index)

    def __iter__(self):
        raise TypeError("'TaskRef' object is not iterable")

@dataclass(frozen=True)
class TaskNode:
    """
    A task, with links to all its inputs or children

    TaskNodes compare equal and hash based on the identity of the task being
    described.

    Attributes:
        task_def: the name and definition of the task
        inputs: list of either task nodes or task references required to
            reconstruct this tasks' result
    """
    task_def: TaskDef | QueryDef
    inputs: 'list[Task]'

    @property
    def name(self) -> TaskName:
        """
        Shortcut
        """
        return self.task_def.name

    @property
    def scatter(self) -> int | None:
        """
        Shortcut for nested scatter field
        """
        return self.task_def.scatter

    def __iter__(self):
        """
        If the tasks was declared to return a tuple of statically known length,
        returns a generator yielding sub-task objects allowing to refer to
        individual members of the result independently.
        """
        if self.scatter is None:
            raise TypeError(
                    'cannot unpack non-iterable Task (no \'scatter\' value given)'
                    )
        return (make_child_task(self, i) for i in
                range(self.scatter))

    def __getitem__(self, index):
        if self.scatter is not None:
            if index >= self.scatter:
                raise IndexError('scatter task index out of range')
            return make_child_task(self, index)
        # Hard getitem, we actually insert an extra task
        return getitem(self, index)

    def __eq__(self, other: object) -> bool:
        match other:
            case TaskNode():
                return self.name == other.name
            case _:
                return False

    def __hash__(self):
        return hash(self.name)

    def __repr__(self):
        return (
                f'TaskNode(task_def={self.task_def!r},'
                f' inputs=[{"..." if self.inputs else ""}])'
                )

    def tree_print(self, prefix: str = '', seen=None) -> str:
        """
        Debug-friendly printing
        """
        if seen is None:
            seen = set()

        if self.name in seen:
            return f'{prefix}{self.name} (see above)\n'
        seen.add(self.name)

        tdef = self.task_def
        match tdef:
            case CoreTaskDef():
                def_str = f'Step {tdef.step}'
            case ChildTaskDef():
                def_str = f'Index {tdef.index} of:'
            case LiteralTaskDef():
                def_str = 'Literal'
            case QueryDef():
                def_str = 'Query'

        string = f'{prefix}{self.name} {def_str}\n'
        for dep in self.inputs:
            string += dep.tree_print(prefix + '  ', seen)
        return string

@dataclass(frozen=True, eq=False) # Use TaskNode eq/hash
class LiteralTaskNode(TaskNode):
    """"
    TaskNode for a literal task, includes the serialized literal object
    """
    task_def: LiteralTaskDef
    children: 'list[Task]'
    serialized: 'Serialized'

    def tree_print(self, prefix: str = '', seen=None) -> str:
        """
        Debug-friendly printing
        """
        if seen is None:
            seen = set()

        if self.name in seen:
            return f'{prefix}{self.name} Literal (see above)\n'
        seen.add(self.name)

        string = f'{prefix}{self.name} Literal\n'
        for dep in self.children:
            string += dep.tree_print(prefix + '  ', seen)
        return string

    def __repr__(self):
        assert not self.inputs
        return (
                f'TaskNode(task_def={self.task_def!r},'
                f' children=[{"..." if self.children else ""}],'
                f' serialized=...)'
                )

Task: TypeAlias = TaskNode | TaskRef

# Task creation routines
# =======================

# Generic task creation routines
# -------------------------------

def hash_one(payload: bytes) -> bytes:
    """
    Hash argument with sha256
    """
    return hashlib.sha256(payload).digest()

def obj_to_name(canon_rep: Any) -> TaskName:
    """
    Generate a task name from a canonical representation of task made from basic
    types.

    (Current implementation may not be canonical yet, this is an aspirational
    docstring)
    """
    payload = msgpack.packb(canon_rep)
    name = TaskName(hash_one(payload))
    return name

def ensure_task(obj: Any) -> 'Task':
    """Makes object into a task in some way.

    If not a task, wrap into a Literal task
    """
    match obj:
        case TaskNode() | TaskRef():
            return obj
        case _:
            return make_literal_task(obj)

def make_task_input(obj: Any) -> tuple[TaskInput, Task]:
    """
    Makes object a task or a simple query

    Returns: a tuple (tin, node) where tin is a task input suitable for
        referencing the task while node is the full task object
    """
    if isinstance(obj, TaskNode) and isinstance(obj.task_def, QueryDef):
        # Only allowed query for now, will be extended in the future
        dep, = obj.inputs
        if obj.task_def.query in ['$base']:
            return (
                    TaskInput(op=TaskOp.BASE, name=obj.task_def.subject),
                    dep
                    )
    # Everything else is treated as task to be recursively run and loaded
    task = ensure_task(obj)
    return (
            TaskInput(op=TaskOp.SUB, name=task.name),
            task
            )

T = TypeVar('T', bound=BaseTaskDef)

def make_task_def(cls: type[T], attrs, extra=None) -> T:
    """
    Generate a name from attribute and extra and create a named task def of the
    wanted type
    """
    def _dump_obj_attrs(obj):
        match obj:
            case Resources():
                attrs = dict(vars(obj))
                if not attrs['vm']:
                    del attrs['vm']
                if not attrs['cpu_list']:
                    del attrs['cpu_list']
                return [msgpack.dumps(attrs)]
            case TaskInput():
                return [msgpack.dumps(vars(obj))]
            case _:
                raise NotImplementedError(obj)
    name = obj_to_name(msgpack.dumps((attrs, extra),
        default=_dump_obj_attrs))
    return cls(name=name, **attrs)

# Literal and child tasks
# ------------------------

@dataclass(frozen=True)
class Serialized(GenSerialized[TaskRef]):
    """
    A serialized task result
    """
    # This attribute is already defined in the base class but we need to narrow
    # it explictly for automatic deserialization
    children: tuple[TaskRef, ...]

    def deserialize(self, children: Sequence[object]) -> Ok[object] | LoadError:
        """
        Given the native objects that children are references to, deserialize
        this resource by injecting the corresponding objects into it.
        """
        return TaskSerializer.loads(self.data, children)

class TaskSerializer(Serializer[Task, TaskRef]):
    """
    Serializer that detects task and step objects
    """
    serialized_cls = Serialized

    @classmethod
    def as_nat(cls, obj: Any) -> Task | None:
        match obj:
            case TaskNode() | TaskRef():
                return obj
            case _:
                return None

    # Just for correct typing because our serialization hierarchy is
    # overcomplicated at the moment
    @classmethod
    def dumps(cls, obj: Any, save: Callable[[Task], TaskRef]
              ) -> Serialized:
        ser = super().dumps(obj, save)
        assert isinstance(ser, Serialized)
        return ser

def make_literal_task(obj: Any) -> LiteralTaskNode:
    """
    Build a Literal TaskNode out of an arbitrary python object
    """
    children = []
    def save(task):
        children.append(task)
        return TaskRef(task.name)
    # Nice to have: more robust hashing, but this is enough for most case where
    # literal resources are a good fit (more complex objects would tend to be
    # actual step outputs)
    serialized = TaskSerializer.dumps(obj, save)

    tdef = {'children': [child.name for child in children]}
    # Literals are an exception to naming: they are named by value, so the
    # name is *not* derived purely from the definition object

    return LiteralTaskNode(
            task_def=make_task_def(LiteralTaskDef, tdef, serialized.data),
            inputs=[],
            children=children,
            serialized=serialized,
            )

def make_child_task_def(parent: TaskName, index: int) -> ChildTaskDef:
    """
    Derive a Child TaskDef from a given parent name

    This does not check whether the operation is legal
    """
    return make_task_def(ChildTaskDef, {'parent': parent, 'index': index})

def make_child_task(parent: TaskNode, index: int) -> TaskNode:
    """
    Derive a Child TaskNode from a given task node

    This does not check whether the operation is legal
    """
    assert parent.task_def.scatter is not None
    assert index <= parent.task_def.scatter

    return TaskNode(
            task_def=make_child_task_def(parent.name, index),
            inputs=[parent]
            )

@dataclass
class ResultRef:
    """
    A reference to the successful result of task execution.

    To be created when a task result is committed to storage, and child tasks are
    not run yet but are somewhere defined.
    """
    name: TaskName
    children: tuple[Task, ...]

@dataclass
class FlatResultRef(ResultRef):
    """
    A reference to the successful result of task execution.

    To be created when a task result is committed to storage, and child tasks are
    not run yet but are remotely defined.
    """
    name: TaskName
    children: tuple[TaskRef, ...]

@dataclass
class RecResultRef(ResultRef):
    """
    A reference to the successful result of a task execution and its child tasks,
    recursively.

    The object itself does actually not store any more information than the
    non recursive result ref ; but the constructor requires, and check at run
    time, references to the child tasks.
    """
    def __init__(self, result: ResultRef, children: 'Iterable[RecResultRef]'):
        expected = {t.name for t in result.children}
        received = {t.name for t in children}
        if expected != received:
            raise ValueError('Wrong child results,'
                f' expected {expected}, got {received}')
        super().__init__(result.name, result.children)

# Steps, i.e. Core Task makers
# ============================

MODULE_SEPARATOR: str = '::'

def raise_if_bad_signature(stp: 'Step', args, kwargs) -> None:
    """
    Check arguments number and names so that we can fail when creating a step
    and not only when we run it
    """
    try:
        inspect.signature(stp.function).bind(*args, **kwargs)
    except TypeError as exc:
        raise TypeError(
                f'{stp.function.__qualname__}() {str(exc)}'
                ) from exc

# Public interface variant
def make_task(stp: 'Step', args: tuple = tuple(),
              kwargs: dict[str, Any] | None = None, **resources):
    """
    Create a task from a step and arguments with given resources
    """
    if kwargs is None:
        kwargs = {}
    # Create resource object by merging in defaults
    res_obj = Resources(**(vars(get_resources()) | resources))

    return make_core_task(stp, args, kwargs, res_obj, **stp.task_options)

def make_core_task(stp: 'Step', args: tuple, kwargs: dict[str, Any],
                   resources: Resources,
                   vtag: str | None = None, scatter: int | None = None,
                   ) -> TaskNode:
    """
    Build a core task from a function call
    """
    # Before anything else, type check the call. This ensures we don't wait
    # until actually trying to run the task to realize we're missing
    # arguments.
    raise_if_bad_signature(stp, args, kwargs)

    # Collect all arguments as nodes
    arg_inputs = []
    kwarg_inputs = {}
    tasks = []
    for arg in args:
        tin, task = make_task_input(arg)
        arg_inputs.append(tin)
        tasks.append(task)
    for key, arg in kwargs.items():
        tin, task = make_task_input(arg)
        kwarg_inputs[key] = tin
        tasks.append(task)

    tdef = {
            'args': arg_inputs,
            'kwargs': kwarg_inputs,
            'step': stp.key,
            'vtags': ([ascii(vtag) ] if vtag is not None else []),
            'scatter': scatter,
            'resources': resources
            }

    ndef = make_task_def(CoreTaskDef, tdef)

    return TaskNode(task_def=ndef, inputs=tasks)

class Step:
    """Object wrapping a function that can be called as a pipeline step

    'Step' refer to the function itself, 'Task' to the function with its
    arguments: 'add' is a step, 'add(3, 4)' a task. You can run a step twice,
    with different arguments, but that would be two different tasks, and a task
    is only ever run once.

    Args:
        scatter: int, signal this task eventually returns a collection of items
            objects, and allows to index or iterate over the task to generate
            handle pointing to the individual items.
        items: legacy alias for scatter
    """

    def __init__(self, function: Callable, is_view: bool = False, **task_options):
        self.function = function
        self.key = function.__module__ + MODULE_SEPARATOR + function.__qualname__

        items = task_options.pop('items', None)
        if items is not None:
            if task_options.get('scatter') is not None:
                raise TypeError('Specify either scatter (preferred) or items')
            task_options['scatter'] = items
        self.task_options = task_options

        self.is_view = is_view
        functools.update_wrapper(self, self.function)

    def __call__(self, *args, **kwargs) -> TaskNode:
        """
        Symbolically call the function wrapped by this step, returning a Task
        object representing the eventual result.
        """
        return make_task(self, args, kwargs)

    def __get__(self, obj, objtype=None):
        """
        Normally, a function wrapper can be transparently applied to methods too
        through the descriptor protocol, but it's not clear what galp should do
        in that case, so we explicitly error out.

        The special function is defined anyway because code may rely on it to
        detect that the Step object is a function wrapper.
        """

        raise NotImplementedError('Cannot only wrap functions, not methods')

class StepLoadError(Exception):
    """
    Failed to load given step
    """

def step(*decorated, **options):
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
        return Step(function, **options)

    if decorated:
        return _step_maker(*decorated)
    return _step_maker

def view(*decorated, **options):
    """
    Shortcut to register a view step
    """
    options['is_view'] = True
    return step(*decorated, **options)

def query(subject: Any, query_doc: Any) -> TaskNode:
    """
    Build a Query task node
    """
    subj_node = ensure_task(subject)
    tdef = {'query': query_doc, 'subject': subj_node.name}
    return TaskNode(
            task_def=make_task_def(
                QueryDef, tdef
                ),
            inputs=[subj_node]
            )

def load_step_by_key(key: str) -> Step:
    """
    Try to import the module containing a step and access such step

    Raises:
        StepLoadError: if loading the module or accessing the step fails
    """
    module_name, step_name = key.split(MODULE_SEPARATOR)

    try:
        module = import_module(module_name)
    except ModuleNotFoundError as exc:
        logging.error('Failed loading step %s: %s',
                key, exc.args[0])
        raise StepLoadError(key) from exc

    try:
        return getattr(module, step_name)
    except AttributeError as exc:
        logging.error(
        'Error while loading step %s: could not import find step %s in %s',
                key, step_name, module_name)
        raise StepLoadError(key) from exc

def getitem(obj, index):
    """
    Obtain a part of task
    """
    return _s_getitem(query(obj, '$base'), index)

@step
def _s_getitem(obj, index):
    """
    Task representing obj[index]

    To optimize, we use this with a $base input, so that we can receive
    containers of task refs and return task refs without running and fetching
    the actual task items. But this means we could even receive a TaskRef to
    the container itself, in which case we need to recurse and follow the chain
    of task refs until we get to the actual container.
    """
    match obj:
        case TaskRef():
            task = getitem(obj, index)
            return task
        case _:
            return obj[index]
