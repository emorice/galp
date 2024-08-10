"""
Global resource management
"""

from contextvars import ContextVar, Token
from contextlib import contextmanager
from dataclasses import asdict

from galp.task_defs import ResourceClaim


_resources: ContextVar[ResourceClaim] = ContextVar(
        'galp.default_resources._resources',
        default=ResourceClaim(cpus=1))

def get_resources() -> ResourceClaim:
    """
    Get default resources
    """
    return _resources.get()

def set_resources(claim: ResourceClaim) -> Token[ResourceClaim]:
    """
    Set default resources. This should be avoided, use the `resources` context
    manager instead unless impossible.
    """
    return _resources.set(claim)

def reset_resources(token: Token[ResourceClaim]) -> None:
    """
    Reset default resources. This should be avoided, use the `resources` context
    manager instead unless impossible.
    """
    return _resources.reset(token)

@contextmanager
def resources(claim: ResourceClaim | None = None, **kwargs):
    """
    Contextually set the default resources per pipeline step

    For readability, this can accept either a ResouceClaim object, keyword
    arguments that should be used to initialize a ResourceClaim, or both to
    override specific attributes of a ResourceClaim.
    """
    local_claim = ResourceClaim(**(
            (asdict(claim) if claim is not None else {})
            | kwargs
            ))
    token = set_resources(local_claim)
    yield
    reset_resources(token)
