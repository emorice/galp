"""
Global resource management
"""

from contextvars import ContextVar, Token
from contextlib import contextmanager

from galp.task_types import ResourceClaim


_resources: ContextVar[ResourceClaim] = ContextVar('galp.default_resources._resources')
_resources.set(ResourceClaim(cpus=1))

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
def resources(claim: ResourceClaim):
    """
    Contextually set the default resources per pipeline step
    """
    token = set_resources(claim)
    yield
    reset_resources(token)
