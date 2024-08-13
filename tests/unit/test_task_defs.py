"""
Unit test for task_definitions
"""

from dataclasses import dataclass
import pytest

from galp.task_defs import (
        TaskDef,
        TaskInput,
        TaskOp,
        TaskName,
        ResourceClaim,
        CoreTaskDef,
        ChildTaskDef,
        LiteralTaskDef,
        )
from galp.pack import dump, load

@pytest.mark.parametrize('case', [
    TaskInput(
        op=TaskOp.BASE,
        name=TaskName(b'a'*32)
        ),
    CoreTaskDef(
        name=TaskName(b'a'*32),
        scatter=3,
        step='some_function',
        args=[TaskInput(TaskOp.SUB, TaskName(b'b'*32))],
        kwargs={'opt': TaskInput(TaskOp.BASE, TaskName(b'c'*32))},
        vtags=['0.1: test'],
        resources=ResourceClaim(4),
        ),
    ChildTaskDef(
        name=TaskName(b'a'*32),
        parent=TaskName(b'b'*32),
        index=3,
        ),
    LiteralTaskDef(
        name=TaskName(b'a'*32),
        children=[TaskName(b'b'*32), TaskName(b'c'*32)],
        ),
    # We're missing query, but these can't be serialized yet
    ])
def test_pack_unpack(case):
    """
    Objects can be serialized and reloaded
    """
    obj = case
    msg = dump(obj)
    assert all(isinstance(f, bytes) for f in msg)
    assert len(msg) == 1
    objback, extras = load(type(obj), msg)
    assert not extras, extras
    assert obj == objback
