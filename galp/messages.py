"""
Models for Galp messages

Not in actual use yet
"""

from typing import Literal, Annotated, TypeAlias
from dataclasses import field, dataclass
from typing_extensions import Self
from pydantic import Field

from . import task_types as gtt
from .task_types import TaskName, TaskDef, CoreTaskDef, TaskRef

# Type registration machinery
# ===========================

class MessageType:
    """
    Base class that implement mandatory registration of direct subclasses for
    all its subclass hierarchy.

    Every class that subclasses, directly or indirectly, MessageType, has to
    specify a unique key that identifies it among its direct sister classes.
    Unicity is enforced at class definition time.

    Class methods are available to obtain the key a class has been registered
    with, and crucially which direct subclass of a given class is associated
    with a key.
    """
    _message_registry: dict[bytes, type[Self]] = {}
    _message_type_key: bytes = b'_root'

    def __init_subclass__(cls, /, key: str) -> None:
        # Normalize the key
        b_key = key.lower().encode('ascii')
        # First, register the class in its parent's regitry
        previous = cls._message_registry.get(b_key)
        if previous:
            raise ValueError(f'Message type key {b_key!r} is already used for '
                    + str(previous))
        cls._message_registry[b_key] = cls
        # Save the key, since accesing the parent's registry later is hard
        cls._message_type_key = b_key
        # Then, initialize a new registry that will mask the parent's
        cls._message_registry = {}

    @classmethod
    def message_get_key(cls) -> bytes:
        """
        Get the unique key associated with a class
        """
        return cls._message_type_key

    @classmethod
    def message_get_type(cls, key: bytes) -> type[Self] | None:
        """
        Get the sub-class associated with a unique key
        """
        # The type checker is right that in general, _message_registry could
        # contain a type that is not a sub-type of Self, and complains.
        # In reality, the fact that this cannot happen is application logic
        # guaranteed by the fact that we *reset* the registry each time, and
        # therefore the hierarchy above is never exposed to the class.
        return cls._message_registry.get(key) # type: ignore[return-value]

# Replies
# ========

class BaseReplyValue(MessageType, key='_rvalue'):
    """
    Base class for messages inside a Reply
    """

@dataclass(frozen=True)
class Doing(BaseReplyValue, key='doing'):
    """
    A message signaling that a task has been allocated or started

    Attributes:
        name: the task name
    """
    name: TaskName

    verb: Literal['doing'] = field(default='doing', repr=False)

@dataclass(frozen=True)
class Done(BaseReplyValue, key='done'):
    """
    A message signaling that a task has been succesful run

    Attributes:
        task_def: the task
        children: the child tasks, typically not yet run, generated by the task
            execution
    """
    task_def: TaskDef
    result: gtt.FlatResultRef

    verb: Literal['done'] = field(default='done', repr=False)

    @property
    def name(self) -> TaskName:
        """
        Task name
        """
        return self.task_def.name

@dataclass(frozen=True)
class Failed(BaseReplyValue, key='failed'):
    """
    Signals that the execution of task has failed

    Attributes:
        task_def: the definition of the failed task
    """
    task_def: CoreTaskDef

    verb: Literal['failed'] = field(default='failed', repr=False)

    @property
    def name(self) -> TaskName:
        """
        Task name
        """
        return self.task_def.name

@dataclass(frozen=True)
class Found(BaseReplyValue, key='found'):
    """
    A message notifying that a task was registered, but not yet executed

    Attributes:
        task_def: the task definition
    """
    task_def: TaskDef

    verb: Literal['found'] = field(default='found', repr=False)

    @property
    def name(self) -> TaskName:
        """
        Task name
        """
        return self.task_def.name

@dataclass(frozen=True)
class NotFound(BaseReplyValue, key='notfound'):
    """
    A message indicating that no trace of a task was found

    Attributes:
        name: the task name
    """
    name: TaskName

    verb: Literal['not_found'] = field(default='not_found', repr=False)

@dataclass(frozen=True)
class Put(BaseReplyValue, key='put'):
    """
    A message sending a serialized task result

    Atrributes:
        name: the task name whose result is sent
        data: the serialized result data
        children: the subordinate task references that are linked from within the
            serialized data
    """
    name: TaskName
    data: bytes
    children: list[TaskRef]

    verb: Literal['put'] = field(default='put', repr=False)

ReplyValue = Annotated[
        Done | Failed | Found | NotFound | Put | Doing,
        Field(discriminator='verb')
        ]

# Messages
# ========

class Message(MessageType, key='_core'):
    """
    Base class for core layer messages
    """

# Lifecycle
# ----------

@dataclass(frozen=True)
class Exit(Message, key='exit'):
    """
    A message asking a peer to leave the system
    """

@dataclass(frozen=True)
class Exited(Message, key='exited'):
    """
    Signals that a peer (unexpectedly) exited. This is typically sent by an
    other peer that detected the kill event

    Attributes:
        peer: the local id (pid) of the exited peer
    """
    peer: str

@dataclass(frozen=True)
class Illegal(Message, key='illegal'):
    """
    A message notifying that a previously sent message was malformed

    Attributes:
        reason: an error message
    """
    reason:str

@dataclass(frozen=True)
class Fork(Message, key='fork'):
    """
    A message asking for a new peer, compatible with some resource claim, to be
    created.
    """
    mission: bytes
    resources: gtt.ResourceClaim

@dataclass(frozen=True)
class Ready(Message, key='ready'):
    """
    A message advertising a worker joining the system

    Attributes:
        local_id: a string identifying the worker in a local system, typically the pid
        mission: a bytestring identifying why the peer is joining
    """
    local_id: str
    mission: bytes

@dataclass(frozen=True)
class PoolReady(Message, key='poolReady'):
    """
    A message advertising a pool (forkserver) joining the system

    Attributes:
        cpus: list of cpus made available by said pool.
    """
    cpus: list[int]

# Requests
# --------

@dataclass(frozen=True)
class Get(Message, key='get'):
    """
    A message asking for an already computed resource

    Attributes:
        name: the task name
    """
    name: TaskName

    @property
    def task_key(self) -> bytes:
        """
        Unique request identifier
        """
        return f'get:{self.name.hex()}'.encode('ascii')

    verb = 'get'

@dataclass(frozen=True)
class Stat(Message, key='stat'):
    """
    A message asking if a task is defined or executed

    Attributes:
        name: the task name
    """
    name: TaskName

    @property
    def task_key(self) -> bytes:
        """
        Unique request identifier
        """
        return f'stat:{self.name.hex()}'.encode('ascii')

    verb = 'stat'

@dataclass(frozen=True)
class Submit(Message, key='submit'):
    """
    A message asking for a task to be executed

    Attributes:
        task_def: the task to execute
        resources: to be allocated to the task
    """
    task_def: CoreTaskDef
    resources: gtt.ResourceClaim

    @property
    def task_key(self) -> bytes:
        """
        Unique request identifier
        """
        return f'submit:{self.task_def.name.hex()}'.encode('ascii')

    @property
    def name(self) -> TaskName:
        """
        Unify name with get/stat
        """
        return self.task_def.name

    verb = 'submit'

Request: TypeAlias = Get | Stat | Submit

# Req-rep wrappers
# ----------------

@dataclass(frozen=True)
class Exec(Message, key='exec'):
    """
    A message wrapping a Submit with a resource allocation
    """
    submit: Submit
    resources: gtt.Resources

@dataclass(frozen=True)
class Reply(Message, key='reply'):
    """
    Wraps the result to a request, identifing said request
    """
    request: str
    value: ReplyValue
