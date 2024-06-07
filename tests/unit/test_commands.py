"""
Unit tests for commands
"""

from collections import defaultdict
import pytest

from galp.task_types import TaskRef, TaskName, Serialized
from galp.asyn import run_command
from galp.result import Ok

import galp.commands as gac

def _as_name(num):
    """Helper to build example TaskNames"""
    return TaskName([num] + [0] * 31)

def test_rget() -> None:
    """
    Rget creates correct command graph
    """

    root = TaskRef(_as_name(0))

    cmd = gac.rget(root)

    def _answer(_prim):
        return Ok(Serialized(b'', [], lambda *_: Ok(None)))

    assert run_command(cmd, _answer) == Ok(None)

@pytest.mark.xfail
def test_rget_losange() -> None:
    """
    Rget creates only one primitive in losange fetch
    """
    root = TaskRef(_as_name(0))

    cmd = gac.rget(root)
    count: defaultdict[TaskName, int] = defaultdict(int)

    def _answer(_prim):
        name = _prim.request.name
        count[name] += 1
        if name == _as_name(0):
            return Ok(Serialized(
                b'',
                [TaskRef(_as_name(1)), TaskRef(_as_name(2))],
                lambda _, children: Ok(list(children))
                ))
        if name in (_as_name(1), _as_name(2)):
            return Ok(Serialized(
                b'',
                [TaskRef(_as_name(3))],
                lambda _, children: Ok(list(children))
                ))
        if name == _as_name(3):
            return Ok(Serialized(
                b'',
                [],
                lambda *_: Ok(None)
                ))
        assert False, 'Bad name'

    assert run_command(cmd, _answer) == Ok([[None], [None]])
    assert count == {_as_name(i): 1 for i in range(4)}
