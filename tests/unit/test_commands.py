"""
Unit tests for commands
"""

from collections import defaultdict

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

    class MockSerialized(Serialized):
        def deserialize(self, _children):
            return Ok(None)

    def _answer(_prim):
        return Ok(MockSerialized(b'', []))

    assert run_command(cmd, _answer) == Ok(None)

def test_rget_losange() -> None:
    """
    Rget creates only one primitive in losange fetch

      1
     / \\
    0    3
     \\ /
       2
    """
    root = TaskRef(_as_name(0))

    cmd = gac.rget(root)
    count: defaultdict[TaskName, int] = defaultdict(int)
    des_count: defaultdict[TaskName, int] = defaultdict(int)

    class MockSerialized(Serialized):
        def __init__(self, *args, tid, **kwargs):
            super().__init__(*args, **kwargs)
            self.tid = tid
        def deserialize(self, children):
            des_count[self.tid] += 1
            return Ok(list(children)) if children else Ok(None)
    def _node(name, children: list[int]):
        return Ok(MockSerialized(
            b'',
            tuple(TaskRef(_as_name(i)) for i in children),
            tid=name))

    def _answer(_prim):
        name = _prim.request.name
        count[name] += 1
        if name == _as_name(0):
            return _node(0, [1, 2])
        if name == _as_name(1):
            return _node(1, [3])
        if name == _as_name(2):
            return _node(2, [3])
        if name == _as_name(3):
            return _node(3, [])
        assert False, 'Bad name'

    assert run_command(cmd, _answer) == Ok([[None], [None]])
    assert count == {_as_name(i): 1 for i in range(4)}
    assert des_count == {i: 1 for i in range(4)}

def test_rget_multiple() -> None:
    """
    Rget creates only one primitive with multiple simultaneaous references

      1
     /
    0
     \\
       1
    """
    root = TaskRef(_as_name(0))

    cmd = gac.rget(root)
    des_count: defaultdict[TaskName, int] = defaultdict(int)

    class MockSerialized(Serialized):
        def __init__(self, *args, tid, **kwargs):
            super().__init__(*args, **kwargs)
            self.tid = tid
        def deserialize(self, children):
            des_count[self.tid] += 1
            return Ok(list(children)) if children else Ok(None)
    def _node(name, children: list[int]):
        return Ok(MockSerialized(b'',
                    [TaskRef(_as_name(i)) for i in children],
                    tid=name))
    def _answer(_prim):
        name = _prim.request.name
        if name == _as_name(0):
            return _node(0, [1, 1])
        if name == _as_name(1):
            return _node(1, [])
        assert False, 'Bad name'

    assert run_command(cmd, _answer) == Ok([None, None])
    assert des_count == {i: 1 for i in range(2)}
