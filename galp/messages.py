"""
Models for Galp messages

Not in actual use yet
"""

from typing import Literal, Annotated
from enum import Enum
from pydantic import BaseModel, Field, PlainSerializer

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

class Ready(BaseModel):
    """
    A message advertising a peer joining the system

    Attributes:
        local_id: a string identifying the worker in a local system, typically the pid
        mission: a bytestring identifying why the peer is joining
    """
    msg: Literal['ready'] = Field('ready', repr=False)

    role: Annotated[Role, PlainSerializer(lambda x: x.value)]
    local_id: str
    mission: bytes

Message = Ready
