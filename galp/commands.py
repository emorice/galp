"""
Lists of internal commands
"""

from enum import Enum
from collections import deque

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
        self.new_commands = deque()

    def collect(self, parent, names, allow_failures=False):
        """
        Creates a command representing a collection of recursive fetches
        """
        return Collect(self, parent, names, allow_failures)

    def run(self, parent, name, task):
        """
        Creates a unique-by-name command representing a task submission and
        fetch
        """
        return self.do_once('RUN', name, parent, task)

    def rsubmit(self, parent, name, task):
        """
        Creates a unique-by-name command representing a task submission
        """
        return self.do_once('RSUBMIT', name, parent, task)

    def rget(self, parent, name):
        """
        Creates a unique-by-name command representing a recursive resource fetch
        """
        return self.do_once('RGET', name, parent)

    def get(self, parent, name):
        """
        Creates a unique-by-name command representing the fetch of a single
        resource
        """
        return self.do_once('GET', name, parent)

    def do_once(self, verb, name, parent, *args, **kwargs):
        """
        Creates a unique command
        """
        key =  verb, name
        cls = {
            'GET': Get,
            'RGET': Rget,
            }[verb]

        if key in self.commands:
            cmd = self.commands[key]
            cmd.outputs.add(parent)
            return cmd
        cmd = cls(self, parent, name, *args, **kwargs)
        self.commands[key] = cmd
        self.new_commands.append(key)
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
    def __init__(self, script, parent):
        self.script = script
        self.status = Status.PENDING
        self.result = None
        self.outputs = set()
        self.update()

        if parent:
            self.outputs.add(parent)

    def _eval(self):
        raise NotImplementedError

    def change_status(self, new_status):
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
                command.update()

    def update(self):
        """
        Re-evals condition and possibly trigger callbacks, returns a list of
        replies
        """
        if self.status == Status.PENDING:
            self.change_status(self._eval())

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
        self.change_status(status)

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
        return Status.PENDING

class Rget(Command):
    """
    Recursively gets a resource and all its parts
    """
    def __init__(self, script, parent, name):
        self.name = name
        super().__init__(script, parent)

    def __str__(self):
        return f'rget {self.name}'

    def _eval(self):
        """
        Eval the trigger condition on the given concrete values
        """
        get = self.script.get(self, self.name)

        if get.is_failed():
            self.result = get.result

        if get.status != Status.DONE:
            return get.status

        rgets = [
            self.script.rget(self, child_name)
            for child_name in get.result
            ]

        for child in rgets:
            if child.is_failed():
                self.result = child.result

        return status_conj(rgets)

class Collect(Command):
    """
    A collection of recursive gets
    """
    def __init__(self, script, parent, names, allow_failures):
        self.names = names
        self.allow_failures = allow_failures
        super().__init__(script, parent)

    def __str__(self):
        return f'collect {self.names}'

    def _eval(self):
        rgets = [
            self.script.rget(self, name)
            for name in self.names
            ]
        if self.allow_failures:
            return status_conj_any(rgets)

        for child in rgets:
            if child.is_failed():
                self.result = child.result

        return status_conj(rgets)

class Callback(Command):
    """
    An arbitrary callback function, used to tie in other code
    """
    def __init__(self, command, callback):
        self._callback = callback
        self._in = command
        self._in.outputs.add(self)
        super().__init__(script=None, parent=None)

    def __str__(self):
        return f'callback {self._callback}'

    def _eval(self):
        if self._in.status != Status.PENDING:
            self._callback(self._in.status)
            return Status.DONE
        return Status.PENDING

class Rsubmit(Command):
    """
    A task submission
    """
    def __init__(self, script, parent, name, task):
        self.name = name
        self.task = task
        super().__init__(script, parent)

    def __str__(self):
        return f'submit {self.name}'

    def _eval(self):
        dep_subs = [
            self.script.rsubmit(self, dep.name)
            for dep in self.task.dependencies
            ]

        for dep in dep_subs:
            if dep.is_failed():
                self.result = dep.result

        return status_conj(dep_subs)

class Run(Command):
    """
    Combined rsubmit + rget
    """
    def __init__(self, script, parent, name, task):
        self.name = name
        self.task = task
        super().__init__(script, parent)

    def __str__(self):
        return f'run {self.name}'

    def _eval(self):
        rsub = self.script.rsubmit(self, self.name, self.task)

        if rsub.is_failed():
            self.result = rsub.result

        if rsub.status != Status.DONE:
            return rsub.status

        rget = self.script.rget(self, self.name)

        if rget.is_failed():
            self.result = rget.result

        return rget.status
