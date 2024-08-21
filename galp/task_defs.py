"""
Task defs: flat objects describing tasks
"""

from enum import Enum
from typing import Union, Any, TypeAlias
from dataclasses import dataclass, KW_ONLY

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

class TaskOp(str, Enum):
    """
    Modifiers to apply to a Task reference
    """
    SUB = '$sub'
    BASE = '$base'

    def __repr__(self):
        return self.value

@dataclass
class TaskInput:
    """
    An object that describes how a task depends on an other task.

    For now, the only allowed formats are:
     * '$sub', task : means that the task should be run recursively and the final
        result passed as input to the downstream task. In some places '$sub' is
        omitted for backward compatibility.
     * '$base', task : means that the task should be run non-recursively and the
        immediate result passed as input to the downstream task
    """
    op: TaskOp
    name: TaskName

@dataclass
class BaseTaskDef:
    """
    Base class for task def objects

    Attributes:
        scatter: number of child tasks statically allowed
    """
    _: KW_ONLY
    name: TaskName
    scatter: int | None = None

    @property
    def inputs(self) -> list[TaskInput]:
        """
        Dependencies (inputs) referenced inside this definition.
        """
        raise NotImplementedError

@dataclass(frozen=True)
class ResourceClaim:
    """
    Resources claimed by a task
    """
    cpus: int
    vm: str = ''

@dataclass
class CoreTaskDef(BaseTaskDef):
    """
    Information defining a core Task, i.e. bound to the remote execution of a
    function
    """
    step: str
    args: list[TaskInput]
    kwargs: dict[str, TaskInput]
    vtags: list[str] # ideally ascii
    resources: ResourceClaim

    @property
    def inputs(self) -> list[TaskInput]:
        return [*self.args, *self.kwargs.values()]

@dataclass
class ChildTaskDef(BaseTaskDef):
    """
    Information defining a child Task, i.e. one item of Task returning a tuple.
    """
    parent: TaskName
    index: int

    @property
    def inputs(self) -> list[TaskInput]:
        # SubTask, the one input is the parent task
        return [TaskInput(op=TaskOp.BASE, name=self.parent)]

@dataclass
class LiteralTaskDef(BaseTaskDef):
    """
    Information defining a literal Task, i.e. a constant.
    """
    children: list[TaskName]

    @property
    def inputs(self) -> list[TaskInput]:
        # Literals are like constant meta-steps, they have children but no
        # inputs
        return []

@dataclass
class QueryDef(BaseTaskDef):
    """
    Information defining a query Task, i.e. a task with a modifier.

    There is some overlap with a TaskInput, in that TaskInputs are essentially a
    subset of queries that are legal to use as inputs to an other task. Ideally
    any query should be usable and both types could be merged back.

    Also, child tasks could eventually be implemented as queries too.
    """
    subject: TaskName
    query: Any # Query type is not well specified yet

    @property
    def inputs(self) -> list[TaskInput]:
        raise NotImplementedError

TaskDef: TypeAlias = Union[CoreTaskDef, ChildTaskDef, LiteralTaskDef]
