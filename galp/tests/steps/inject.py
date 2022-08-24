"""
Tests steps related to injection
"""

from galp.graph import StepSet

from .utils import identity

export = StepSet()

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

export.bind(injected_list=[identity(5), identity(7)])

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

export.bind(none_value=None)
