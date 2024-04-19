"""
Unit tests for commands
"""

from galp.task_types import TaskRef, TaskName, Serialized
from galp.asyn import run_command
from galp.result import Ok

import galp.commands as gac

def test_rget() -> None:
    """
    Rget creates correct command graph
    """

    def _as_name(num):
        return TaskName([0] * 31 + [num])

    root = TaskRef(_as_name(0))

    cmd = gac.rget(root)

    def _answer(_prim):
        return Ok(Serialized(b'', [], lambda *_: Ok(None)))

    assert run_command(cmd, _answer) == Ok(None)
