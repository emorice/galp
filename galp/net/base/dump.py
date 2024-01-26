"""
General serializing helpers and types
"""

from typing import TypeVar, Callable, TypeAlias

M = TypeVar('M')

Writer: TypeAlias = Callable[[M], list[bytes]]
"""
Function that converts M to a list of bytes. This is defined as a type alias:
    * as a shortcut because Callable is verbose, and
    * as a central point to define what is the result type of the serialization
"""
