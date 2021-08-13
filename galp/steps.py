"""
Default steps

Internal use steps are prefixed with 'galp_'
Todo: many of these are just for test, find a way to package them away cleanly
and make sure they are never called in prod.
"""
import logging

from galp.graph import StepSet

export = StepSet()

@export.step
def galp_hello():
    logging.warning('Task "Hello" running')
    return 42

@export.step
def galp_double(x=1):
    logging.warn('Task "Double" running')
    return 2*x

@export.step
def galp_sub(a, b):
    """Computes a - b, trivial non commutative function"""
    return a - b
