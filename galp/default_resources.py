"""
Global resource management
"""

from contextvars import ContextVar, Token
from contextlib import contextmanager
from dataclasses import asdict

from galp.task_defs import Resources


_resources: ContextVar[Resources] = ContextVar(
        'galp.default_resources._resources',
        default=Resources(cpus=1))

def get_resources() -> Resources:
    """
    Get default resources
    """
    return _resources.get()

def set_resources(claim: Resources) -> Token[Resources]:
    """
    Set default resources. This should be avoided, use the `resources` context
    manager instead unless impossible.
    """
    return _resources.set(claim)

def reset_resources(token: Token[Resources]) -> None:
    """
    Reset default resources. This should be avoided, use the `resources` context
    manager instead unless impossible.
    """
    return _resources.reset(token)

@contextmanager
def resources(claim: Resources | None = None, **kwargs):
    """
    Contextually set the default resources per pipeline step

    For readability, this can accept either a ResouceClaim object, keyword
    arguments that should be used to initialize a Resources, or both to
    override specific attributes of a Resources.

    Priority order: first the keyword arguments, then the attributes of the
    claim argument, then the former default resources.
    """
    upper = get_resources()
    local_claim = Resources(**(
            asdict(upper)
            | (asdict(claim) if claim is not None else {})
            | kwargs
            ))
    token = set_resources(local_claim)
    yield
    reset_resources(token)
