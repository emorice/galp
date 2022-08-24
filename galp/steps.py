"""
Default steps

Internal use steps are prefixed with 'galp_'
Todo: many of these are just for test, find a way to package them away cleanly
and make sure they are never called in prod.
"""
import logging

from galp.graph import StepSet

export = StepSet()

@export
def getitem(obj, index):
    """
    Task representing obj[index]
    """
    return obj[index]

# The steps below are mostly for testing, they should be moved somewhere else
# ===

@export.step
def galp_hello():
    logging.info('Task "Hello" running')
    return 42

@export.step
def galp_double(x=1):
    logging.info('Task "Double" running')
    return 2*x

@export.step
def galp_sub(a, b):
    """Computes a - b, trivial non commutative function"""
    return a - b
