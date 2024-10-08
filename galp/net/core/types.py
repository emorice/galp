"""
Core galp message types.

Core messages are essentially the first level of message hierarchy, and are
therefore a mix a messages related to very different big groups of
functionalities.
"""

from types import GenericAlias
from typing import TypeVar, Generic, TypeAlias, TypedDict, Literal
from dataclasses import dataclass

import galp.task_types as gtt

from galp.result import Ok, Progress, Error

# Messages
# ========

# Lifecycle
# ----------

@dataclass(frozen=True)
class Exited:
    """
    Signals that a peer (unexpectedly) exited. This is typically sent by an
    other peer that detected the kill event

    Attributes:
        peer: the local id (pid) of the exited peer
        error: any additional details to report to user
    """
    peer: str
    error: str

@dataclass(frozen=True)
class Fork:
    """
    A message asking for a new peer, compatible with some resource claim, to be
    created.
    """
    mission: bytes
    resources: gtt.Resources

@dataclass(frozen=True)
class Ready:
    """
    A message advertising a worker joining the system

    Attributes:
        local_id: a string identifying the worker in a local system, typically the pid
        mission: a bytestring identifying why the peer is joining
    """
    local_id: str
    mission: bytes

@dataclass(frozen=True)
class PoolReady:
    """
    A message advertising a pool (forkserver) joining the system

    Attributes:
        cpus: list of cpus made available by said pool.
    """
    cpus: list[int]

# Requests
# --------

V = TypeVar('V', covariant=True) # pylint: disable=typevar-name-incorrect-variance

# pylint: disable=too-few-public-methods
class BaseRequest(Generic[V]):
    """
    Logical base request class.

    Generic gives the type of the expected response.
    """
    reply_type: type[V]

    @property
    def input_id(self) -> gtt.TaskName:
        """Unique key to identify request input"""
        raise NotImplementedError

    def __class_getitem__(cls, item):
        orig = GenericAlias(cls, item)
        class _Request(orig):
            reply_type = item

            @property
            def input_id(self) -> gtt.TaskName:
                """Unique key to identify request input"""
                raise NotImplementedError
        return _Request

@dataclass(frozen=True)
class Get(BaseRequest[gtt.Serialized]):
    """
    A message asking for an already computed resource

    Attributes:
        name: the task name
    """
    name: gtt.TaskName

    @property
    def input_id(self) -> gtt.TaskName:
        return self.name

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
    task_def: gtt.TaskDef | None
    result: gtt.FlatResultRef | None

@dataclass(frozen=True)
class Stat(BaseRequest[StatResult]):
    """
    A message asking if a task is defined or executed

    Attributes:
        name: the task name
    """
    name: gtt.TaskName

    @property
    def input_id(self) -> gtt.TaskName:
        return self.name

@dataclass(frozen=True)
class Submit(BaseRequest[gtt.FlatResultRef]):
    """
    A message asking for a task to be executed

    Attributes:
        task_def: the task to execute
        resources: an override to the resource claim in task_def, used e.g. to
            allocate specific cpus
    """
    task_def: gtt.CoreTaskDef
    resources: gtt.Resources | None = None

    @property
    def input_id(self) -> gtt.TaskName:
        return self.task_def.name

@dataclass(frozen=True)
class Upload(BaseRequest[gtt.FlatResultRef]):
    """
    Ask for a task result to be written in store
    """
    task_def: gtt.TaskDef
    payload: gtt.Serialized

    @property
    def input_id(self) -> gtt.TaskName:
        return self.task_def.name

Request: TypeAlias = Get | Stat | Submit | Upload

@dataclass(frozen=True)
class RequestId:
    """
    The unique identifier of a request, excluding payloads
    """
    verb: bytes
    name: gtt.TaskName

    def as_word(self) -> bytes:
        """
        Converts self to a printable, space-less string
        """
        return self.verb + f':{self.name.hex()}'.encode('ascii')

    @classmethod
    def from_word(cls, word: bytes):
        """
        Parse from a byte string
        """
        verb, _, hexname = word.partition(b':')
        return cls(
                verb,
                gtt.TaskName.fromhex(hexname.decode('ascii'))
                )

# Req-rep wrappers
# ----------------

class RemoteError(Error[str]):
    """Remote encountered an error when processing"""

@dataclass(frozen=True)
class Reply(Generic[V]):
    """
    Wraps the result to a request, identifing said request
    """
    request: RequestId
    value: Ok[V] | Progress | RemoteError

class TaskProgress(TypedDict):
    """
    Type of status attribute of Progress for Submit requests.

    This type is not enforced by the messaging layer, it's a convention between
    progress info producers and consumers.
    """
    event: Literal['started', 'stdout', 'stderr']
    payload: bytes

@dataclass(frozen=True)
class NextRequest:
    """Event sent by broker when ready for next request"""

Message: TypeAlias = (
        Exited | Fork | Ready | PoolReady |
        Request | Reply | NextRequest
        )
