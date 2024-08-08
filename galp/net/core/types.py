"""
Core galp message types.

Core messages are essentially the first level of message hierarchy, and are
therefore a mix a messages related to very different big groups of
functionalities.
"""

from types import GenericAlias
from typing import TypeVar, Generic, TypeAlias
from dataclasses import dataclass

import galp.task_types as gtt
import galp.net.requests.types as gr

from galp.result import Ok, Progress
from galp.net.base.types import MessageType
from galp.net.requests.types import RemoteError

# Messages
# ========

# Lifecycle
# ----------

@dataclass(frozen=True)
class Exited(MessageType, key='exited'):
    """
    Signals that a peer (unexpectedly) exited. This is typically sent by an
    other peer that detected the kill event

    Attributes:
        peer: the local id (pid) of the exited peer
    """
    peer: str

@dataclass(frozen=True)
class Fork(MessageType, key='fork'):
    """
    A message asking for a new peer, compatible with some resource claim, to be
    created.
    """
    mission: bytes
    resources: gtt.ResourceClaim

@dataclass(frozen=True)
class Ready(MessageType, key='ready'):
    """
    A message advertising a worker joining the system

    Attributes:
        local_id: a string identifying the worker in a local system, typically the pid
        mission: a bytestring identifying why the peer is joining
    """
    local_id: str
    mission: bytes

@dataclass(frozen=True)
class PoolReady(MessageType, key='poolReady'):
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
class BaseRequest(MessageType, Generic[V], key=None):
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
        class _Request(orig, key=None):
            reply_type = item

            @property
            def input_id(self) -> gtt.TaskName:
                """Unique key to identify request input"""
                raise NotImplementedError
        return _Request

@dataclass(frozen=True)
class Get(BaseRequest[gtt.Serialized], key='get'):
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
class Stat(BaseRequest[gr.StatReplyValue], key='stat'):
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
class Submit(BaseRequest[gtt.FlatResultRef], key='submit'):
    """
    A message asking for a task to be executed

    Attributes:
        task_def: the task to execute
        resources: to be allocated to the task
    """
    task_def: gtt.CoreTaskDef

    @property
    def input_id(self) -> gtt.TaskName:
        return self.task_def.name

@dataclass(frozen=True)
class Upload(BaseRequest[gtt.FlatResultRef], key='upload'):
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

def get_request_id(req: BaseRequest) -> RequestId:
    """
    Make request id from Request
    """
    return RequestId(req.message_get_key(), req.input_id)

# Req-rep wrappers
# ----------------

@dataclass(frozen=True)
class Exec(MessageType, key='exec'):
    """
    A message wrapping a Submit with a resource allocation
    """
    submit: Submit
    resources: gtt.Resources

@dataclass(frozen=True)
class Reply(MessageType, Generic[V], key='reply'):
    """
    Wraps the result to a request, identifing said request
    """
    request: RequestId
    value: Ok[V] | Progress | RemoteError

@dataclass(frozen=True)
class NextRequest(MessageType, key='next_request'):
    """Event sent by broker when ready for next request"""

Message: TypeAlias = (
        Exited | Fork | Ready | PoolReady |
        Request |
        Exec | Reply | NextRequest
        )
