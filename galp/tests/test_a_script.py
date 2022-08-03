"""
Tests for Script
"""

from galp.script import Script, AllDone, Command

def test_add_alldone():
    """
    Tests adding a command with no deps
    """
    script = Script()
    cdef = script.define_command(
        'test',
        trigger=AllDone()
        )
    _cmd = script.run(cdef)

    assert script.status('test') == Command.DONE
