"""
Core galp message types.

Core messages are essentially the first level of message hierarchy, and are
therefore a mix a messages related to very different big groups of
functionalities.
"""

from typing import TypeVar, Generic, TypeAlias
from dataclasses import dataclass

import galp.task_types as gtt
from galp.net.base.types import MessageType
from galp.net.requests.types import ReplyValue
import galp.net.requests.types as gr

# Messages
# ========

# Lifecycle
# ----------

@dataclass(frozen=True)
class Exit(MessageType, key='exit'):
    """
    A message asking a peer to leave the system
    """

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

V = TypeVar('V', bound=ReplyValue)

# pylint: disable=too-few-public-methods
class Request(MessageType, Generic[V], key=None):
    """
    Logical base request class.

    Generic gives the type of the expected response.
    """
    def __class_getitem__(cls, item):
        class _Request(MessageType, key=None):
            reply_type = item
        return _Request

@dataclass(frozen=True)
class Get(Request[gr.GetReplyValue], key='get'):
    """
    A message asking for an already computed resource

    Attributes:
        name: the task name
    """
    name: gtt.TaskName
    verb = 'get'

@dataclass(frozen=True)
class Stat(Request[gr.StatReplyValue], key='stat'):
    """
    A message asking if a task is defined or executed

    Attributes:
        name: the task name
    """
    name: gtt.TaskName
    verb = 'stat'

@dataclass(frozen=True)
class Submit(Request[gr.SubmitReplyValue], key='submit'):
    """
    A message asking for a task to be executed

    Attributes:
        task_def: the task to execute
        resources: to be allocated to the task
    """
    task_def: gtt.CoreTaskDef
    resources: gtt.ResourceClaim

    @property
    def name(self) -> gtt.TaskName:
        """
        Unify name with get/stat
        """
        return self.task_def.name

    verb = 'submit'

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

def get_request_id(req: Request) -> RequestId:
    """
    Make request id from Request
    """
    return RequestId(req.message_get_key(), req.name)

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
    value: V

Message: TypeAlias = (
        Exit | Exited | Fork | Ready | PoolReady |
        Get | Stat | Submit
        | Exec | Reply
        )
