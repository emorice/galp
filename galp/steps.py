"""
Default steps

Internal use steps are prefixed with 'galp_'
"""
import logging

from galp.graph import StepSet

export = StepSet()

@export.step
def galp_hello():
    logging.warn('Task "Hello" running')
    return 42

@export.step
def galp_double(x=1):
    logging.warn('Task "Double" running')
    return 2*x
