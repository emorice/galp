"""
Galp test steps used to build other steps
"""

import galp

export = galp.Block()

@export
def identity(arg):
    """
    Returns its arg unchanged
    """
    print(arg)
    return arg
