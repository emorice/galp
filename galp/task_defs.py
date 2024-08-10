"""
Task defs: flat objects describing tasks
"""

from enum import Enum
from typing import Union, Any, Annotated, Literal
from dataclasses import dataclass

from pydantic_core import CoreSchema, core_schema
from pydantic import GetCoreSchemaHandler, BaseModel, Field, PlainSerializer

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
        omitted for backward compatibility.
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
    resources: ResourceClaim

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
