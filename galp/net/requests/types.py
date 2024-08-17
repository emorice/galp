"""
Models for Galp messages
"""

from dataclasses import dataclass

from galp.result import Error
from galp.task_types import TaskDef, FlatResultRef

# Replies
# ========

@dataclass(frozen=True)
class StatResult:
    """
    Unified stat reply

    Attributes:
        task_def: the task definition if present in the store from a remote
            creation of previous execution attempt
        result: the task result reference if the task has already been run
            succesfully. Normally this should never be found set if the task def
            is not.
    """
    task_def: TaskDef | None
    result: FlatResultRef | None

# All-purpose error
# =================

class RemoteError(Error[str]):
    """Remote encountered an error when processing"""
