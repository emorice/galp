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
from pydantic import BaseModel, model_validator

from galp.result import Ok
from galp.task_defs import (TaskName, TaskDef, LiteralTaskDef, TaskInput,
                            ResourceClaim, TaskOp, BaseTaskDef, CoreTaskDef,
                            ChildTaskDef, QueryTaskDef)
from galp.default_resources import get_resources
from galp.serializer import Serializer, GenSerialized, dump_model, LoadError

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

    def tree_print(self, indent: int = 0) -> str:
        """
        Debug-friendly printing
        """
        _ = indent
        return repr(self)

@dataclass(frozen=True)
class TaskNode:
    """
    A task, with links to all its dependencies

    Dependencies here means $sub-dep, including children. TaskNodes compare
    equal and hash based on the identity of the task being described.

    Attributes:
        task_def: the name and definition of the task
        dependencies: list of either task nodes or task references required to
            reconstruct this tasks' result
        data: constant value of the task, for Literal tasks
    """
    task_def: TaskDef
    dependencies: 'list[Task]'

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
        return (make_child_task(self, i) for i in
                range(self.scatter))

    def __getitem__(self, index):
        if self.scatter is not None:
            assert index <= self.scatter, 'Indexing past declared scatter'
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

    def tree_print(self, indent: int = 0) -> str:
        """
        Debug-friendly printing
        """
        pad = '    ' * indent
        if not self.dependencies:
            return pad + repr(self)
        string = f'{pad}TaskNode(task_def={repr(self.task_def)},'
        string += ' dependencies=[\n'
        for dep in self.dependencies:
            string += dep.tree_print(indent + 1)
            string += '\n'
        string += pad + '])'
        return string

@dataclass(frozen=True, eq=False) # Use TaskNode eq/hash
class LiteralTaskNode(TaskNode):
    """"
    TaskNode for a literal task, includes the serialized literal object
    """
    task_def: LiteralTaskDef
    serialized: 'Serialized'

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

def ensure_task_node(obj: Any) -> 'TaskNode':
    """Makes object into a task in some way.

    If it's a step, try to call it to get a task.
    Else, wrap into a Literal task
    """
    if isinstance(obj, TaskNode):
        return obj
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
        default=dump_model))
    return cls(name=name, **attrs)

# Literal and child tasks
# ------------------------

class Serialized(GenSerialized[TaskRef]):
    """
    A serialized task result
    """
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
    dependencies = []
    def save(task):
        dependencies.append(task)
        return TaskRef(task.name)
    # Nice to have: more robust hashing, but this is enough for most case where
    # literal resources are a good fit (more complex objects would tend to be
    # actual step outputs)
    serialized = TaskSerializer.dumps(obj, save)

    tdef = {'children': [dep.name for dep in dependencies]}
    # Literals are an exception to naming: they are named by value, so the
    # name is *not* derived purely from the definition object

    return LiteralTaskNode(
            task_def=make_task_def(LiteralTaskDef, tdef, serialized.data),
            dependencies=dependencies,
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
            dependencies=[parent]
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

class ReadyCoreTaskDef(BaseModel):
    """
    A core task def, along with a ResultRef for each input.

    Typical of a task that is ready to be executed since all its dependencies are
    fulfilled
    """
    task_def: CoreTaskDef
    inputs: dict[TaskName, ResultRef]

    @model_validator(mode='after')
    def check_inputs(self) -> 'ReadyCoreTaskDef':
        """
        Checks that the provided inputs match the expected inputs
        """
        if ({tin.name for tin in self.task_def.dependencies(TaskOp.BASE)}
                != self.inputs.keys):
            raise ValueError('Wrong inputs')
        return self

@dataclass(frozen=True)
class Resources:
    """
    Resources available or allocated to a task
    """
    cpus: list[int]

    def allocate(self, claim: ResourceClaim
            ) -> tuple['Resources | None', 'Resources']:
        """
        Try to split resources specified by claim off this resource set

        Returns:
            tuple (allocated, rest). If resources are insufficient, `rest` is
            unchanged and `allocated` will be None.
        """
        if claim.cpus > len(self.cpus):
            return None, self

        alloc_cpus, rest_cpus = self.cpus[:claim.cpus], self.cpus[claim.cpus:]

        return Resources(alloc_cpus), Resources(rest_cpus)

    def free(self, resources):
        """
        Return allocated resources
        """
        return Resources(self.cpus + resources.cpus)

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

def make_core_task(stp: 'Step', args: tuple, kwargs: dict[str, Any],
                   resources: ResourceClaim,
                   vtag: str | None = None, items: int | None = None,
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
    nodes = []
    for arg in args:
        tin, node = ensure_task_input(arg)
        arg_inputs.append(tin)
        nodes.append(node)
    for key, arg in kwargs.items():
        tin, node = ensure_task_input(arg)
        kwarg_inputs[key] = tin
        nodes.append(node)

    tdef = {
            'args': arg_inputs,
            'kwargs': kwarg_inputs,
            'step': stp.key,
            'vtags': ([ascii(vtag) ] if vtag is not None else []),
            'scatter':items,
            'resources': resources
            }

    ndef = make_task_def(CoreTaskDef, tdef)

    return TaskNode(task_def=ndef, dependencies=nodes)

class Step:
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

    def __init__(self, function: Callable, is_view: bool = False, **task_options):
        self.function = function
        self.key = function.__module__ + MODULE_SEPARATOR + function.__qualname__
        self.task_options = task_options
        self.is_view = is_view
        functools.update_wrapper(self, self.function)

    def __call__(self, *args, **kwargs) -> TaskNode:
        """
        Symbolically call the function wrapped by this step, returning a Task
        object representing the eventual result.
        """
        return make_core_task(self, args, kwargs, get_resources(), **self.task_options)

    def __get__(self, obj, objtype=None):
        """
        Normally, a function wrapper can be transparently applied to methods too
        through the descriptor protocol, but it's not clear what galp should do
        in that case, so we explicitly error out.

        The special function is defined anyway because code may rely on it to
        detect that the Step object is a function wrapper.
        """

        raise NotImplementedError('Cannot only wrap functions, not methods')

class NoSuchStep(ValueError):
    """
    No step with the given name has been registered
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
    subj_node = ensure_task_node(subject)
    tdef = {'query': query_doc, 'subject': subj_node.name}
    return TaskNode(
            task_def=make_task_def(
                QueryTaskDef, tdef
                ),
            dependencies=[subj_node]
            )

def load_step_by_key(key: str) -> Step:
    """
    Try to import the module containing a step and access such step

    Raises:
        NoSuchStep: if loading the module or accessing the step fails
    """
    module_name, step_name = key.split(MODULE_SEPARATOR)

    try:
        module = import_module(module_name)
    except ModuleNotFoundError as exc:
        logging.error('Error while loading step %s: could not import module %s',
                key, module_name)
        raise NoSuchStep(key) from exc

    try:
        return getattr(module, step_name)
    except AttributeError as exc:
        logging.error(
        'Error while loading step %s: could not import find step %s in %s',
                key, step_name, module_name)
        raise NoSuchStep(key) from exc

@step
def getitem(obj, index):
    """
    Task representing obj[index]
    """
    return obj[index]
