"""
Abstract task types defintions
"""

from abc import ABC, abstractmethod
from typing import Any, Literal, Union, Annotated, TypeAlias
from enum import Enum
from dataclasses import dataclass
from functools import total_ordering

from pydantic_core import CoreSchema, core_schema
from pydantic import GetCoreSchemaHandler, BaseModel, Field, PlainSerializer

import galp
from . import graph

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

@dataclass
class TaskReference:
    """
    A reference to an existing task by name, stripped of all the task definition
    details.

    The only valid operation on such a task is to read its name.
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
        return (graph.make_child_task(self, i) for i in
                range(self.scatter))

    def __getitem__(self, index):
        if self.scatter is not None:
            assert index <= self.scatter, 'Indexing past declared scatter'
            return graph.make_child_task(self, index)
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


Task: TypeAlias = TaskNode | TaskReference

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

@total_ordering
class Resources(BaseModel):
    """
    Resources claimed by a task
    """
    cpus: int = Field(ge=0)

    def __sub__(self, other: 'Resources') -> 'Resources':
        return Resources(cpus=self.cpus - other.cpus)

    def __add__(self, other: 'Resources') -> 'Resources':
        return Resources(cpus=self.cpus + other.cpus)

    def __ge__(self, other: 'Resources') -> bool:
        return self.cpus >= other.cpus
