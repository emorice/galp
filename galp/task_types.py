"""
Abstract task types defintions
"""
from abc import ABC, abstractmethod
from typing import Any, Literal, Union
from enum import Enum
from dataclasses import dataclass

from pydantic_core import CoreSchema, core_schema
from pydantic import GetCoreSchemaHandler, BaseModel, Field


class TaskName(bytes):
    """
    Simpler wrapper around bytes with a shortened, more readable repr
    """
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
        return core_schema.no_info_after_validator_function(cls, handler(bytes))

class TaskOp(str, Enum):
    """
    Modifiers to apply to a Task reference
    """
    SUB = '$sub'
    BASE = '$base'

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
    op: TaskOp
    name: TaskName

class BaseTaskDef(BaseModel):
    """
    Base class for task def objects
    """
    task_type: str

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

class CoreTaskDef(BaseTaskDef):
    """
    Information defining a core Task, i.e. bound to the remote execution of a
    function
    """
    task_type: Literal['core']

    args: list[TaskInput]
    kwargs: dict[str, TaskInput]
    step: bytes
    vtags: list[str] # ideally ascii

    def dependencies(self, mode: TaskOp) -> list[TaskInput]:
        # Only base deps
        return [*self.args, *self.kwargs.values()]

class ChildTaskDef(BaseTaskDef):
    """
    Information defining a child Task, i.e. one item of Task returning a tuple.
    """
    task_type: Literal['child']

    parent: TaskName
    index: int

    def dependencies(self, mode: TaskOp) -> list[TaskInput]:
        # SubTask, the (base) dependency is the parent task
        return [ TaskInput(op='$base', name=self.parent) ]

class LiteralTaskDef(BaseTaskDef):
    """
    Information defining a literal Task, i.e. a constant.
    """
    task_type: Literal['child']

    children: list[TaskName]

    def dependencies(self, mode: TaskOp) -> list[TaskInput]:
        match mode:
            case TaskOp.BASE:
                return []
            case TaskOp.SUB:
                return self.children

class QueryTaskDef(BaseTaskDef):
    """
    Information defining a query Task, i.e. a task with a modifier.

    There is some overlap with a TaskInput, in that TaskInputs are essentially a
    subset of queries that are legal to use as inputs to an other task. Ideally
    any query should be usable and both types could be merged back.

    Also, child tasks could eventually be implemented as queries too.
    """
    task_type: Literal['query']

    subject: TaskName
    query: Any # Query type is not well specified yet

TaskDef = Union[CoreTaskDef, ChildTaskDef, LiteralTaskDef, QueryTaskDef]

class NamedTaskDef(BaseModel):
    """
    A task to which a reproducible unique name has been given by a naming
    procedure
    """
    name: TaskName
    task_def: TaskDef = Field(discriminator='task_type')

@dataclass
class TaskNode:
    """
    A task, with links to all its dependencies

    Dependencies here means $sub-dep, including children.

    """
    named_def: NamedTaskDef
    dependencies: dict[TaskName, 'TaskNode']
    data: Any = None

    @property
    def name(self) -> TaskName:
        """
        Shortcut
        """
        return self.named_def.name

    def __iter__(self):
        """
        If the tasks was declared to return a tuple of statically known length,
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

class StepType(ABC):
    """
    Root of step type hierarchy
    """

    @abstractmethod
    def __call__(self, *args, **kwargs) -> TaskNode:
        raise NotImplementedError
