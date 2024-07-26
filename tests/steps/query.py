"""
Steps for testing the query system
"""

import galp

@galp.step
def do_nothing(arg):
    """
    No-op step
    """
    del arg

@galp.step
def do_meta():
    """
    Meta-step
    """
    return [do_nothing(3), do_nothing(4)]

@galp.step
def index_type(obj, index):
    """
    Potential base meta-step
    """
    return str(type(obj[index]))
