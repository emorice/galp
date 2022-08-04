"""
Lists of internal commands
"""

from enum import Enum
import logging
from galp.graph import SubTask

def status_conj(commands):
    """
    Ternary conjunction of an iterable of status
    """
    status = [cmd.status for cmd in commands]
    if all(s == Status.DONE for s in status):
        return Status.DONE
    if any(s == Status.FAILED for s in status):
        return Status.FAILED
    return Status.UNKNOWN

def status_conj_any(commands):
    """
    Ternary conjunction of an iterable of status
    """
    status = [cmd.status for cmd in commands]
    if all(s != Status.UNKNOWN for s in status):
        return Status.DONE
    return Status.UNKNOWN


class Script:
    """
    A collection of maker methods for Commands
    """
    def __init__(self):
        self.commands = {}

    def collect(self, parent, names, allow_failures=False):
        """
        Creates a command representing a collection of recursive fetches
        """
        return Collect(self, parent, names, allow_failures)

    def rget(self, parent, name):
        """
        Creates a non-unique command representing a recursive resource fetch
        """
        return Rget(self, parent, name)

    def get(self, parent, name):
        """
        Creates a unique-by-name command representing the fetch of a single
        resource
        """
        key = 'GET', name
        if key in self.commands:
            cmd = self.commands[key]
            cmd.outputs.append(parent)
            return cmd
        cmd = Get(self, parent, name)
        self.commands[key] = cmd
        return cmd

    def callback(self, command, callback):
        """
        Adds an arbitrary callback to an existing command
        """
        return Callback(command, callback)

class Status(Enum):
    """
    Command status
    """
    UNKNOWN ='U'
    DONE = 'D'
    FAILED = 'F'

class Command:
    """
    An asynchronous command
    """
    def __init__(self, script, parent):
        logging.info('START\t%s', self)
        self.script = script
        self._status = Status.UNKNOWN
        self.result = None
        self.outputs = []
        self.update()

        if parent:
            self.outputs.append(parent)

    def _eval(self):
        raise NotImplementedError

    @property
    def status(self):
        """
        Status (unknown, done or failed) of the command
        """
        return self._status

    @status.setter
    def status(self, new_status):
        old_status = self._status
        self._status = new_status
        if old_status == Status.UNKNOWN and new_status != Status.UNKNOWN:
            logging.info('%s\t%s',
                new_status.name + ' ' * (
                    len(Status.FAILED.name) - len(new_status.name)
                    ),
                self
                )
            for command in self.outputs:
                command.update()

    def update(self):
        """
        Re-evals condition and possibly trigger callbacks
        """
        logging.info('UP    \t%s', self)
        if self.status != Status.UNKNOWN:
            return
        self.status = self._eval()

    def done(self, result):
        """
        Mark the future as done and runs callbacks.

        A task can be safely marked several times, the callbacks are run only
        when the status transitions from unknown to known.

        However successive done calls will overwrite the result.
        """
        self.result = result
        self.status = Status.DONE

    def failed(self, result):
        """
        Mark the future as failed and runs callbacks.

        A task can be safely marked several times, the callbacks are run only
        when the status transitions from unknown to known.

        However successive done calls will overwrite the result.
        """
        self.result = result
        self.status = Status.FAILED

class Get(Command):
    """
    Get a single resource part
    """
    def __init__(self, script, parent, name):
        self.name = name
        super().__init__(script, parent)

    def __str__(self):
        return f'get {self.name}'

    def _eval(self):
        return Status.UNKNOWN

class Rget(Command):
    """
    Recursively gets a resource and all its parts
    """
    def __init__(self, script, parent, name):
        self.name = name
        self._in_get = None
        self._in_rgets = None
        super().__init__(script, parent)

    def __str__(self):
        return f'rget {self.name}'

    def _eval(self):
        """
        Eval the trigger condition on the given concrete values
        """
        if self._in_get is None:
            self._in_get = self.script.get(self, self.name)

        if self._in_get.status != Status.DONE:
            return self._in_get.status

        if self._in_rgets is None:
            self._in_rgets = [
                self.script.rget(self, SubTask.gen_name(self.name, i))
                for i in range(self._in_get.result)
                ]
        return status_conj(self._in_rgets)

class Collect(Command):
    """
    A collection of recursive gets
    """
    def __init__(self, script, parent, names, allow_failures):
        self.names = names
        self.allow_failures = allow_failures
        self._in_rgets = None
        super().__init__(script, parent)

    def __str__(self):
        return f'collect {self.names}'

    def _eval(self):
        if self._in_rgets is None:
            self._in_rgets = [
                self.script.rget(self, name)
                for name in self.names
                ]
        if self.allow_failures:
            return status_conj_any(self._in_rgets)
        return status_conj(self._in_rgets)

class Callback(Command):
    """
    An arbitrary callback function, used to tie in other code
    """
    def __init__(self, command, callback):
        self._callback = callback
        self._in = command
        self._in.outputs.append(self)
        super().__init__(script=None, parent=None)

    def __str__(self):
        return f'callback {self._callback}'

    def _eval(self):
        if self._in.status != Status.UNKNOWN:
            self._callback(self._in.status)
            return Status.DONE
        return Status.UNKNOWN
