"""
Lists of internal commands
"""

from enum import Enum
import logging

def status_conj(commands):
    """
    Ternary conjunction of an iterable of status
    """
    status = [cmd.status for cmd in commands]
    if all(s == Status.DONE for s in status):
        return Status.DONE
    if any(s == Status.FAILED for s in status):
        return Status.FAILED
    return Status.PENDING

def status_conj_any(commands):
    """
    Ternary conjunction of an iterable of status
    """
    status = [cmd.status for cmd in commands]
    if all(s != Status.PENDING for s in status):
        return Status.DONE
    return Status.PENDING


class Script:
    """
    A collection of maker methods for Commands
    """
    def __init__(self):
        self.commands = {}

    def collect(self, parent, out, names, allow_failures=False):
        """
        Creates a command representing a collection of recursive fetches
        """
        return Collect(self, parent, out, names, allow_failures)

    def rget(self, parent, out, name):
        """
        Creates a non-unique command representing a recursive resource fetch
        """
        return Rget(self, parent, out, name)

    def get(self, parent, out, name):
        """
        Creates a unique-by-name command representing the fetch of a single
        resource
        """
        key = 'GET', name
        if key in self.commands:
            cmd = self.commands[key]
            cmd.outputs.append(parent)
            return cmd
        cmd = Get(self, parent, out, name)
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
    PENDING ='P'
    DONE = 'D'
    FAILED = 'F'

class Command:
    """
    An asynchronous command
    """
    def __init__(self, script, parent, out):
        self.script = script
        self.status = Status.PENDING
        self.result = None
        self.outputs = []
        self.update(out)

        if parent:
            self.outputs.append(parent)

    def _eval(self, out):
        raise NotImplementedError

    def change_status(self, new_status, out):
        """
        Updates status, triggering callbacks and collecting replies
        """
        old_status = self.status
        self.status = new_status
        logging.info('%s\t%s',
            new_status.name + ' ' * (
                len(Status.FAILED.name) - len(new_status.name)
                ),
            self
            )
        if old_status == Status.PENDING and new_status != Status.PENDING:
            for command in self.outputs:
                command.update(out)

    def update(self, out):
        """
        Re-evals condition and possibly trigger callbacks, returns a list of
        replies
        """
        if self.status == Status.PENDING:
            self.change_status(self._eval(out), out)

    def done(self, result):
        """
        Mark the future as done and runs callbacks.

        A task can be safely marked several times, the callbacks are run only
        when the status transitions from unknown to known.

        However successive done calls will overwrite the result.
        """
        return self._mark_as(Status.DONE, result)

    def is_done(self):
        """
        Boolean, if command done
        """
        return self.status == Status.DONE

    def is_failed(self):
        """
        Boolean, if command done
        """
        return self.status == Status.FAILED

    def is_pending(self):
        """
        Boolean, if command still pending
        """
        return self.status == Status.PENDING

    def failed(self, result):
        """
        Mark the future as failed and runs callbacks.

        A task can be safely marked several times, the callbacks are run only
        when the status transitions from unknown to known.

        However successive done calls will overwrite the result.
        """
        return self._mark_as(Status.FAILED, result)

    def _mark_as(self, status, result):
        self.result = result
        out = []
        self.change_status(status, out)
        return out

class Get(Command):
    """
    Get a single resource part
    """
    def __init__(self, script, parent, out, name):
        self.name = name
        out.append(('GET', name))
        super().__init__(script, parent, out)

    def __str__(self):
        return f'get {self.name}'

    def _eval(self, out):
        return Status.PENDING

class Rget(Command):
    """
    Recursively gets a resource and all its parts
    """
    def __init__(self, script, parent, out, name):
        self.name = name
        self._in_get = None
        self._in_rgets = None
        super().__init__(script, parent, out)

    def __str__(self):
        return f'rget {self.name}'

    def _eval(self, out):
        """
        Eval the trigger condition on the given concrete values
        """
        if self._in_get is None:
            self._in_get = self.script.get(self, out, self.name)

        if self._in_get.status != Status.DONE:
            return self._in_get.status

        if self._in_rgets is None:
            self._in_rgets = [
                self.script.rget(self, out, child_name)
                for child_name in self._in_get.result
                ]
        return status_conj(self._in_rgets)

class Collect(Command):
    """
    A collection of recursive gets
    """
    def __init__(self, script, parent, out, names, allow_failures):
        self.names = names
        self.allow_failures = allow_failures
        self._in_rgets = None
        super().__init__(script, parent, out)

    def __str__(self):
        return f'collect {self.names}'

    def _eval(self, out):
        if self._in_rgets is None:
            self._in_rgets = [
                self.script.rget(self, out, name)
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
        super().__init__(script=None, parent=None, out=None)

    def __str__(self):
        return f'callback {self._callback}'

    def _eval(self, out):
        if self._in.status != Status.PENDING:
            self._callback(self._in.status)
            return Status.DONE
        return Status.PENDING
