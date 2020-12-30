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
