"""
Galp test steps used to build other steps
"""

from galp.graph import StepSet

export = StepSet()

@export
def identity(arg):
    """
    Returns its arg unchanged
    """
    return arg
