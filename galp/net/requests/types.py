"""
Models for Galp messages
"""

from dataclasses import dataclass

from galp.result import Error
from galp.net.base.types import MessageType
from galp.task_types import TaskDef, FlatResultRef

# Replies
# ========

@dataclass(frozen=True)
class ReplyValue(MessageType, key='_rvalue'):
    """
    Base class for messages inside a Reply
    """

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
class Progress(ReplyValue, key='progress'):
    """
    Value giving a progress report
    """
    status: str

# All-purpose error
# =================

class RemoteError(Error[str]):
    """Remote encountered an error when processing"""
