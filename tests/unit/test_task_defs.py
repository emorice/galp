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
        CoreTaskDef,
        ChildTaskDef,
        )
from galp.pack import dump, load

@pytest.mark.parametrize('case', [
    TaskInput(
        op=TaskOp.BASE,
        name=TaskName(b'a'*32)
        ),
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
