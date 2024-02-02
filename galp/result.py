"""
Simple tagged unions of types with error types to use as return value
"""

import traceback
from typing import TypeAlias, Generic, TypeVar, Callable
from dataclasses import dataclass
from typing_extensions import Self

OkT = TypeVar('OkT')
R = TypeVar('R')

@dataclass(frozen=True)
class Ok(Generic[OkT]):
    """
    Succesful result variant
    """
    value: OkT

    def then(self, function: Callable[[OkT], R]) -> R:
        """Unpack Ok type"""
        return function(self.value)

ErrMessageT = TypeVar('ErrMessageT')

class Error(Exception, Generic[ErrMessageT]):
    """
    Alternative to exception meant to be returned rather than thrown

    This object collects a stack trace that can be printed, but because "true"
    tracebacks are not meant to be created from python, this makes no attempt
    to match the Exception interface. It inherits Exception purely for typing
    purposes.

    The error attributes is intended to be a human-readable addition, i.e.
    ErrMessageT is commonly just str
    """
    error: ErrMessageT
    stack_summary: list[traceback.FrameSummary]

    def then(self, _function) -> Self:
        """Propagate Error type"""
        return self

    def __init__(self, error: ErrMessageT):
        self.error = error
        self.stack_summary = traceback.extract_stack()[:-1]

    def __str__(self):
        """
        This error with its traceback
        """
        return ''.join(traceback.format_list(self.stack_summary) + [
            f'{self.__class__.__module__}.{self.__class__.__qualname__}: {self.error}'
            ])

ErrT = TypeVar('ErrT', bound=Error)

Result: TypeAlias = Ok[OkT] | ErrT
