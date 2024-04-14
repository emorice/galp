"""
Unit tests for asyn
"""

import galp.asyn as ga
from galp.result import Ok

class TestPrimitive(ga.Primitive):
    """
    Mock primitive
    """
    def __init__(self, value):
        super().__init__()
        self.value = value

    @property
    def key(self):
        return self.value

def test_run_command():
    """
    run_command return correct result
    """
    command = TestPrimitive(1).then(lambda x: 2 * x)

    # FIXME: that shouldn't be the type of that callback !
    assert ga.run_command(command, lambda prim: prim.value) == 2
