"""
Unit tests for asyn
"""

import galp.asyn as ga
from galp.result import Ok

class MockPrimitive(ga.Primitive):
    """
    Mock primitive
    """
    def __init__(self, value):
        self.value = value

    @property
    def key(self):
        return self.value

def test_run_command():
    """
    run_command return correct result
    """
    command = MockPrimitive(1).then(lambda x: 2 * x)

    assert ga.run_command(command, lambda prim: Ok(prim.value)) == 2
