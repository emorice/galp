"""
Abstract task types defintions
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
import hashlib
from typing import (Any, Literal, Union, Annotated, TypeAlias, TypeVar, Generic,
        Iterable)

import msgpack # type: ignore[import] # Issue #85
from pydantic_core import CoreSchema, core_schema
from pydantic import (GetCoreSchemaHandler, BaseModel, Field, PlainSerializer,
        model_validator)

from galp.serializer import Serializer
from galp.serialize import Serialized
import galp.steps

# Core task type definitions
# ==========================

# Task defs: flat objects decribing tasks
# ----------------------------------------

class TaskName(bytes):
    """
    Simpler wrapper around bytes with a shortened, more readable repr
    """
    SIZE = 32
    def __new__(cls, content):
        bts = super().__new__(cls, content)
        if len(bts) != cls.SIZE:
            raise TypeError(f'TaskName must be {cls.SIZE} bytes long')
        return bts

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        _hex = self.hex()
        return _hex[:7]

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: Any, handler: GetCoreSchemaHandler
    ) -> CoreSchema:
        """
        Pydantic magic straight out of the docs to validate as bytes
        """
        _ = source_type
        return core_schema.no_info_after_validator_function(cls, handler(bytes))

class TaskOp(str, Enum):
    """
    Modifiers to apply to a Task reference
    """
    SUB = '$sub'
    BASE = '$base'

    def __repr__(self):
        return self.value

class TaskInput(BaseModel):
    """
    An object that describes how a task depends on an other task.

    For now, the only allowed formats are:
     * '$sub', task : means that the task should be run recursively and the final
        result passed as input to the downstream task. In some places '$sub' is
        ommited for bacward compatibility.
     * '$base', task : means that the task should be run non-recursively and the
        immediate result passed as input to the downstream task
    """
    op: Annotated[TaskOp, PlainSerializer(lambda op: op.value)]
    name: TaskName

class BaseTaskDef(BaseModel):
    """
    Base class for task def objects

    Attributes:
        scatter: number of child tasks statically allowed
    """
    name: TaskName
    scatter: int | None = None

    def dependencies(self, mode: TaskOp) -> list[TaskInput]:
        """
        Dependencies referenced inside this definition.

        If mode is '$base', this returns only tasks that are upstream of this
        task, that is, task whose succesful completion is required before this
        task can run.

        If mode is '$sub', this returns also the children tasks, that is, the
        tasks whose execution is independent from this task, but are referenced
        by this task and whose value would eventually be needed to construct the
        full result of this task.

        Note that since this is only from the definition, only statically known
        children tasks can be returned. Running the task may generate further
        dynamic child tasks.
        """
        raise NotImplementedError

# False positive https://github.com/pydantic/pydantic/issues/3125
class CoreTaskDef(BaseTaskDef): # type: ignore[no-redef]
    """
    Information defining a core Task, i.e. bound to the remote execution of a
    function
    """
    task_type: Literal['core'] = Field('core', repr=False)

    step: str
    args: list[TaskInput]
    kwargs: dict[str, TaskInput]
    vtags: list[str] # ideally ascii

    def dependencies(self, mode: TaskOp) -> list[TaskInput]:
        # Only base deps
        return [*self.args, *self.kwargs.values()]

class ChildTaskDef(BaseTaskDef):
    """
    Information defining a child Task, i.e. one item of Task returning a tuple.
    """
    task_type: Literal['child'] = Field('child', repr=False)

    parent: TaskName
    index: int

    def dependencies(self, mode: TaskOp) -> list[TaskInput]:
        # SubTask, the (base) dependency is the parent task
        return [ TaskInput(op=TaskOp.BASE, name=self.parent) ]

class LiteralTaskDef(BaseTaskDef):
    """
    Information defining a literal Task, i.e. a constant.
    """
    task_type: Literal['literal'] = Field('literal', repr=False)

    children: list[TaskName]

    def dependencies(self, mode: TaskOp) -> list[TaskInput]:
        match mode:
            case TaskOp.BASE:
                return []
            case TaskOp.SUB:
                return [TaskInput(op=TaskOp.SUB, name=child) for child in
                        self.children]

class QueryTaskDef(BaseTaskDef):
    """
    Information defining a query Task, i.e. a task with a modifier.

    There is some overlap with a TaskInput, in that TaskInputs are essentially a
    subset of queries that are legal to use as inputs to an other task. Ideally
    any query should be usable and both types could be merged back.

    Also, child tasks could eventually be implemented as queries too.
    """
    task_type: Literal['query'] = Field('query', repr=False)

    subject: TaskName
    query: Any # Query type is not well specified yet

    def dependencies(self, mode: TaskOp) -> list[TaskInput]:
        raise NotImplementedError

TaskDef = Annotated[
        Union[CoreTaskDef, ChildTaskDef, LiteralTaskDef, QueryTaskDef],
        Field(discriminator='task_type')
        ]

# Tasks: possibly recursive objects representing graphs of tasks
# --------------------------------------------------------------

@dataclass
class TaskRef:
    """
    A reference to a defined task by name.

    This is intended to be created when a task definition is commited to
    storage, so that a task reference exists if and only if a task definition
    has been stored succesfully.

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

@dataclass
class TaskNode:
    """
    A task, with links to all its dependencies

    Dependencies here means $sub-dep, including children.

    Attributes:
        task_def: the name and definition of the task
        dependencies: list of either task nodes or task references required to
            reconstruct this tasks' result
        data: constant value of the task, for Literal tasks
    """
    task_def: TaskDef
    dependencies: 'list[Task]'
    data: Any = None

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
        return galp.steps.getitem(self, index)

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

    (Current implememtation may not be canonical yet, this is an aspirational
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

# Literal and child tasks
# ------------------------

# pylint: disable=too-few-public-methods
# This is a forward declaration of graph.Step
# Issue #82
class StepType(ABC):
    """
    Root of step type hierarchy
    """

    @abstractmethod
    def __call__(self, *args, **kwargs) -> TaskNode:
        raise NotImplementedError

class TaskSerializer(Serializer[Task, TaskRef]):
    """
    Serializer that detects task and step objects
    """
    @classmethod
    def as_nat(cls, obj: Any) -> Task | None:
        match obj:
            case StepType():
                return obj()
            case TaskNode() | TaskRef():
                return obj
            case _:
                return None

SerializedTask: TypeAlias = Serialized[TaskRef]

def make_literal_task(obj: Any) -> TaskNode:
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
    obj_bytes, _dependencies_refs = TaskSerializer.dumps(obj, save)

    tdef = {'children': [dep.name for dep in dependencies]}
    # Literals are an exception to naming: they are named by value, so the
    # name is *not* derived purely from the definition object

    return TaskNode(
            task_def=make_task_def(LiteralTaskDef, tdef, obj_bytes),
            dependencies=dependencies,
            data=obj,
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
TaskT_co = TypeVar('TaskT_co', TaskRef, Task, covariant=True)

@dataclass
class GenericResultRef(Generic[TaskT_co]):
    """
    A reference to the sucessful result of task execution.

    Not recursive, the task can have child tasks still to be executed. On the
    other hand, child tasks must be at least registered, as shown by the
    children field being typed as TaskRef.

    This is intended to be created when a task result is commited to storage, so
    that a result reference exists if and only if a task has been run and the
    result stored succesfully.

    We have two instances of this template: with a TaskRef, it is serializable
    and can be included in messages. With an arbitary Task object (i.e., with
    TaskNodes too), it is intended only for local use since we never want to
    automatically serialize a whole task graph.
    """
    name: TaskName
    children: list[TaskT_co]

ResultRef: TypeAlias = GenericResultRef[Task]
FlatResultRef: TypeAlias = GenericResultRef[TaskRef]

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

    Typical of a task that is ready to be excuted since all its dependencies are
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
class ResourceClaim:
    """
    Resources claimed by a task
    """
    cpus: int = Field(ge=0)

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
