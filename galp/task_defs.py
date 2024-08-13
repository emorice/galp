"""
Task defs: flat objects describing tasks
"""

from enum import Enum
from typing import Union, Any, Annotated, Literal
from dataclasses import dataclass, field, KW_ONLY

from pydantic_core import CoreSchema, core_schema
from pydantic import GetCoreSchemaHandler, Field

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

    def dependencies(self, mode: TaskOp) -> list[TaskInput]:
        """
        Dependencies referenced inside this definition.

        If mode is '$base', this returns only tasks that are upstream of this
        task, that is, task whose successful completion is required before this
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

@dataclass(frozen=True)
class ResourceClaim:
    """
    Resources claimed by a task
    """
    cpus: int = Field(ge=0)

@dataclass
class CoreTaskDef(BaseTaskDef):
    """
    Information defining a core Task, i.e. bound to the remote execution of a
    function
    """
    _: KW_ONLY
    step: str
    args: list[TaskInput]
    kwargs: dict[str, TaskInput]
    vtags: list[str] # ideally ascii
    resources: ResourceClaim

    task_type: Literal['core'] = field(default='core', repr=False)

    def dependencies(self, mode: TaskOp) -> list[TaskInput]:
        # Only base deps
        return [*self.args, *self.kwargs.values()]

@dataclass
class ChildTaskDef(BaseTaskDef):
    """
    Information defining a child Task, i.e. one item of Task returning a tuple.
    """

    _: KW_ONLY
    parent: TaskName
    index: int

    task_type: Literal['child'] = field(default='child', repr=False)

    def dependencies(self, mode: TaskOp) -> list[TaskInput]:
        # SubTask, the (base) dependency is the parent task
        return [ TaskInput(op=TaskOp.BASE, name=self.parent) ]

@dataclass
class LiteralTaskDef(BaseTaskDef):
    """
    Information defining a literal Task, i.e. a constant.
    """
    _: KW_ONLY
    children: list[TaskName]

    task_type: Literal['literal'] = field(default='literal', repr=False)

    def dependencies(self, mode: TaskOp) -> list[TaskInput]:
        match mode:
            case TaskOp.BASE:
                return []
            case TaskOp.SUB:
                return [TaskInput(op=TaskOp.SUB, name=child) for child in
                        self.children]

@dataclass
class QueryTaskDef(BaseTaskDef):
    """
    Information defining a query Task, i.e. a task with a modifier.

    There is some overlap with a TaskInput, in that TaskInputs are essentially a
    subset of queries that are legal to use as inputs to an other task. Ideally
    any query should be usable and both types could be merged back.

    Also, child tasks could eventually be implemented as queries too.
    """
    _: KW_ONLY
    subject: TaskName
    query: Any # Query type is not well specified yet

    task_type: Literal['query'] = field(default='query', repr=False)

    def dependencies(self, mode: TaskOp) -> list[TaskInput]:
        raise NotImplementedError

TaskDef = Annotated[
        Union[CoreTaskDef, ChildTaskDef, LiteralTaskDef, QueryTaskDef],
        Field(discriminator='task_type')
        ]
