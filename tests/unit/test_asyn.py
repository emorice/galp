"""
Unit tests for asyn
"""

import pytest

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
    command = MockPrimitive(1).then(lambda x: Ok(2 * x))

    assert ga.run_command(command, lambda prim: Ok(prim.value)) == Ok(2)

@pytest.mark.xfail
def test_run_command_double_ref():
    """
    run_command handles the same command being referenced twice
    """
    count = [0]
    def _double(x: int) -> Ok[int]:
        print('Doubling !', flush=True)
        count[0] += 1
        return Ok(2 * x)
    two = MockPrimitive(1).then(_double)

    five = (MockPrimitive(1)
             # First ref
             .then(lambda x : two.then(lambda t: Ok(x + t)))
             # Second ref
             .then(lambda x : two.then(lambda t: Ok(x + t)))
             )

    # 1 + 2 + 2
    assert ga.run_command(five, lambda prim: Ok(prim.value)) == Ok(5)
    assert count[0] == 1
