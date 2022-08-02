"""
Tests for Script
"""

from galp.script import Script, AllDone, Done

def test_add_alldone():
    """
    Tests adding a command with no deps
    """
    script = Script()
    script.add_command(
        'test',
        trigger=AllDone()
        )

    assert script.status('test') == Done.TRUE
