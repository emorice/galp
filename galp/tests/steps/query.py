"""
Steps for testing the query system
"""

import galp

blk = galp.Block()

@blk.step
def do_nothing(arg):
    """
    No-op step
    """
    del arg
