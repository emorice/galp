"""
Models for Galp messages

Not in actual use yet
"""

from typing import Literal, Annotated, TypeVar
from enum import Enum
from pydantic import BaseModel, Field, PlainSerializer

from .lower_protocol import Route
from .task_types import TaskName

def task_key(msg: list[bytes]) -> bytes:
    """
    Generate an identifier for the message
    """
    # Most messages are unique per verb + first arg
    return msg[0] + msg[1]

class Role(str, Enum):
    """
    Enum identifying the role of a peer in the system
    """
    POOL = 'pool'
    WORKER = 'worker'

M = TypeVar('M', bound='BaseMessage')

class BaseMessage(BaseModel):
    """
    Base class for messages
    """
    incoming: Route
    forward: Route
    verb: str

    @classmethod
    def reply(cls: type[M], other: 'BaseMessage', **kwargs) -> M:
        """
        Contruct a message from the given args, extracting and swapping the
        incoming and forward routes from `other`
        """
        return cls(incoming=other.forward, forward=other.incoming, **kwargs)

    @classmethod
    def plain_reply(cls: type[M], route: tuple[Route, Route], **kwargs) -> M:
        """
        Contruct a message from the given args, extracting and swapping the
        incoming and forward routes from a legacy Route tuple.

        TODO: Compat method, to be removed
        """
        incoming, forward = route
        return cls(incoming=forward, forward=incoming, **kwargs)


class Ready(BaseMessage):
    """
    A message advertising a peer joining the system

    Attributes:
        local_id: a string identifying the worker in a local system, typically the pid
        mission: a bytestring identifying why the peer is joining
    """
    verb: Literal['ready'] = Field('ready', repr=False)

    role: Annotated[Role, PlainSerializer(lambda x: x.value)]
    local_id: str
    mission: bytes

class Put(BaseMessage):
    """
    A message sending a serialized task result

    Atrributes:
        name: the task name whose result is sent
        data: the serialized result data
        children: the subordinate task names that are linked from within the
            serialized data
    """

    verb: Literal['put'] = Field('put', repr=False)

    name: TaskName
    data: bytes
    children: list[TaskName]

Message = Ready | Put
