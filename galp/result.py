"""
Simple tagged unions of types with error types to use as return value
"""

import traceback
import logging
from typing import TypeAlias, Generic, TypeVar, Callable, NoReturn, Iterable
from dataclasses import dataclass
from typing_extensions import Self

OkT = TypeVar('OkT', covariant=True) # pylint: disable=typevar-name-incorrect-variance
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

    def eventually(self, function: 'Callable[[Result[OkT]], R]') -> R:
        """Trivially apply function to self.

        Part of unified interface with async operations
        """
        return function(self)

    def unwrap(self) -> OkT:
        """Unsafe unpacking"""
        return self.value

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

    def eventually(self, function: 'Callable[[Result[OkT]], R]') -> R:
        """Trivially apply function to self.

        Part of unified interface with async operations
        """
        return function(self)

    def unwrap(self) -> NoReturn:
        """Unsafe unpacking"""
        logging.error('Unwrap failed: %s', self)
        raise RuntimeError('Unwrapped failed')

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


Result: TypeAlias = Ok[OkT] | Error

ErrT = TypeVar('ErrT', bound=Error)

@dataclass(frozen=True)
class Progress:
    """
    Class used to carry information about a future Result
    """
    status: str

def all_ok(inputs: Iterable[Ok[OkT] | ErrT]
           ) -> Ok[list[OkT]] | ErrT:
    """
    Return Error if any result is Error, else Ok with the list of values
    """
    results = []

    for result in inputs:
        match result:
            case Ok():
                results.append(result.value)
            case Error():
                return result
    return Ok(results)
