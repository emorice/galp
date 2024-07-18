"""
Tests steps related to injection
"""

import galp

from .utils import identity

export = galp.Block()

# pylint: disable=redefined-outer-name

@export
def hello():
    """
    Injectable constant
    """
    return 'hi'

@export
def uses_inject(hello):
    """
    Has an injectable argument
    """
    return 'Injected ' + hello

@export
def sum_inject(injected_list):
    """
    Sums over an injected list
    """
    return sum(injected_list)

@export
def sum_inject_trans(injected_list_trans):
    """
    Sums over an injected list, again
    """
    return sum(injected_list_trans)

@export
def uses_inject_trans(sum_inject_trans):
    """
    Downstream step to sum_inject_trans
    """
    return sum_inject_trans

@export
def injects_none(none_value):
    """
    Step expecting an actual None as value to inject
    """
    return none_value is None

@export
def has_default(value=42):
    """
    Step with a default argument value

    Injection should detect that nothing is missing
    """
    return value

@export
def uses_has_default(has_default):
    """
    Step using a step with a default through injection
    """
    return has_default

@export
def step_a():
    """
    Nothing
    """

@export
def uses_a(step_a):
    """
    Nothing
    """
    _ = step_a

@export
def uses_a_and_ua(step_a, uses_a):
    """
    Nothing
    """
    _ = step_a, uses_a

@export
def uses_ua_and_a(uses_a, step_a):
    """
    Nothing
    """
    _ = step_a, uses_a

@export
def recursive(recursive):
    """
    Recursive injection, not allowed
    """
    del recursive

@export
def cyclic(co_cyclic):
    """
    Cyclic injection, not allowed
    """
    del co_cyclic

@export
def co_cyclic(cyclic):
    """
    Cyclic injection, not allowed
    """
    del cyclic

@export
def uses_indexed(indexed):
    """
    To inject with an indexed step
    """
    return indexed
