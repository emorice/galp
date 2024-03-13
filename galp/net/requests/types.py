"""
Models for Galp messages
"""

from typing import Any, Callable
from dataclasses import dataclass

from galp.result import Error
from galp.net.base.types import MessageType
from galp.task_types import (
        TaskDef, CoreTaskDef, TaskRef, FlatResultRef
        )
from galp.serialize import Result, LoadError

# Replies
# ========

@dataclass(frozen=True)
class ReplyValue(MessageType, key='_rvalue'):
    """
    Base class for messages inside a Reply
    """

@dataclass(frozen=True)
class Done(ReplyValue, key='done'):
    """
    A message signaling that a task has been succesful run

    Attributes:
        result: name and child tasks, typically not yet run, generated by the
            task execution
    """
    result: FlatResultRef

@dataclass(frozen=True)
class Failed(ReplyValue, key='failed'):
    """
    Signals that the execution of task has failed

    Attributes:
        task_def: the definition of the failed task
    """
    task_def: CoreTaskDef

SubmitReplyValue = Done | Failed

@dataclass(frozen=True)
class Found(ReplyValue, key='found'):
    """
    A message notifying that a task was registered, but not yet executed

    Attributes:
        task_def: the task definition
    """
    task_def: TaskDef

@dataclass(frozen=True)
class NotFound(ReplyValue, key='notfound'):
    """
    A message indicating that no trace of a task was found
    """

@dataclass(frozen=True)
class StatDone(ReplyValue, key='statdone'):
    """
    A message signaling that a task has been succesful run

    Attributes:
        task_def: the task
        children: the child tasks, typically not yet run, generated by the task
            execution
    """
    task_def: TaskDef
    result: FlatResultRef

StatReplyValue = Found | NotFound | StatDone

@dataclass(frozen=True)
class Put(ReplyValue, key='put'):
    """
    A message sending a serialized task result

    Atrributes:
        data: the serialized result data
        children: the subordinate task references that are linked from within the
            serialized data
    """
    data: bytes
    children: list[TaskRef]
    _loads: Callable[[bytes, list[Any]], Any]

    def deserialize(self, children: list[Any]) -> Result[Any, LoadError]:
        """
        Given the native objects that children are references to, deserialize
        this resource by injecting the corresponding objects into it.
        """
        return self._loads(self.data, children)

# All-purpose error
# =================

class RemoteError(Error[str]):
    """Remote encountered an error when processing"""
