"""
Default steps
"""

from galp.graph import step

@step
def getitem(obj, index):
    """
    Task representing obj[index]
    """
    return obj[index]
