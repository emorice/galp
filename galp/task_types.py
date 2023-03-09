"""
Abstract task types defintions
"""
from abc import ABC, abstractmethod
from typing import TypedDict, Any

class TaskName(bytes):
    """
    Simpler wrapper around bytes with a shortened, more readable repr
    """
    def __repr__(self):
        return self.__str__()

    def __str__(self):
        _hex = self.hex()
        return _hex[:7]

TaskInputDoc = tuple[str, TaskName] | TaskName
"""
A document suitable for serialization representing a task input

See TaskInput
"""

class TaskDict(TypedDict, total=False):
    """
    Serializable representation of a task

    The *_names key are a legacy from before task docs were introduced
    """
    # Name of the task, usually computed from the rest of the dict
    name: TaskName

    # Keys for regular tasks
    arg_names: list[TaskInputDoc]
    kwarg_names: dict[str, TaskInputDoc]
    step_name: bytes
    vtags: list[str] # ideally ascii

    # Keys for child tasks
    parent: TaskName

    # Keys for literals
    children: list[TaskName]

    # Keys for queries
    subject: TaskName
    query: Any # Query type is not well specified yet

class TaskType(ABC):
    """
    Root of task type hierarchy
    """
    name: TaskName

    @abstractmethod
    def to_dict(self) -> TaskDict:
        """
        Dict representation of self
        """
        raise NotImplementedError

    @property
    @abstractmethod
    def dependencies(self) -> list['TaskType']:
        """
        Task dependencies
        """
        raise NotImplementedError

TaskInput = tuple[str, TaskType]
"""
An object that describes how a task depends on an other task.

For now, the only allowed formats are:
 * '$sub', task : means that the task should be run recursively and the final
    result passed as input to the downstream task. In some places '$sub' is
    ommited for bacward compatibility.
 * '$base', task : means that the task should be run non-recursively and the
    immediate result passed as input to the downstream task
"""

class StepType(ABC):
    """
    Root of step type hierarchy
    """

    @abstractmethod
    def __call__(self, *args, **kwargs) -> TaskType:
        raise NotImplementedError
