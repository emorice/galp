"""
Lists pf internal commands
"""

from collections import namedtuple
from galp.script import ForLoop

def define_commands(script):
    """
    Define the lazy asynchronous commands used by galp
    """
    commands = namedtuple('GALPCommands', [
            'TASKNAME', 'GET', 'RGET'
        ])(
            script.define_command('TASKNAME'),
            script.define_command('GET'),
            script.define_command('RGET',
                trigger=ForLoop(
                    length='GET',
                    body='RGET'
                )
            )
        )
    return commands
