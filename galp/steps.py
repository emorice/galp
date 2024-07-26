"""
Default steps

Internal use steps are prefixed with 'galp_'
Todo: many of these are just for test, find a way to package them away cleanly
and make sure they are never called in prod.
"""
import logging

from galp.graph import step

@step
def getitem(obj, index):
    """
    Task representing obj[index]
    """
    return obj[index]

# The steps below are mostly for testing, they should be moved somewhere else
# ===

@step
def galp_hello():
    """
    Test step, to be removed
    """
    logging.info('Task "Hello" running')
    return 42

@step
def galp_double(value=1):
    """
    Test step, to be removed
    """
    logging.info('Task "Double" running')
    return 2 * value

@step
def galp_sub(a, b): # pylint: disable=invalid-name # math like operator.sub
    """Computes a - b, trivial non commutative function"""
    return a - b
