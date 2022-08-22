"""
Lists of internal commands
"""

from enum import Enum
from collections import deque

import logging

from .graph import task_deps

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

class Status(Enum):
    """
    Command status
    """
    PENDING = 'P'
    DONE = 'D'
    FAILED = 'F'

    def __bool__(self):
        return self != Status.DONE

_once_commands = {}
def once(cls):
    """
    Register a class for use by `do_once`
    """
    _once_commands[cls.__name__.upper()] = cls
    return cls

class Command:
    """
    An asynchronous command
    """
    def __init__(self, script):
        self.script = script
        self.status = Status.PENDING
        self.result = None
        self.outputs = set()
        self.update(init=True)

    def out(self, cmd):
        """
        Add command to the set of outputs
        """
        self.outputs.add(cmd)

    def _eval(self):
        raise NotImplementedError

    def change_status(self, new_status, init=False):
        """
        Updates status, triggering callbacks and collecting replies
        """
        old_status = self.status
        self.status = new_status
        logging.info('%s %s',
                ('NEW' if init else new_status.name).ljust(7),
                self
                )
        if old_status == Status.PENDING and new_status != Status.PENDING:
            for command in self.outputs:
                command.update()

    def update(self, init=False):
        """
        Re-evals condition and possibly trigger callbacks, returns a list of
        replies
        """
        if self.status == Status.PENDING:
            self.change_status(self._eval(), init)

    def done(self, result=None):
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

    def failed(self, result=None):
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

    def do_once(self, verb, name, *args, **kwargs):
        """
        Creates a unique command
        """
        key =  verb, name
        cls = _once_commands[verb]

        if key in self.script.commands:
            cmd = self.script.commands[key]
            cmd.out(self)
            return cmd
        cmd = cls(self.script, name, *args, **kwargs)
        cmd.out(self)
        self.script.commands[key] = cmd
        self.script.new_commands.append(key)
        return cmd

    def run(self, name):
        """
        Creates a unique-by-name command representing a task submission and
        fetch
        """
        return self.do_once('RUN', name)

    def rsubmit(self, name):
        """
        Creates a unique-by-name command representing a recursive task submission
        """
        return self.do_once('RSUBMIT', name)

    def submit(self, name):
        """
        Creates a unique-by-name command representing a task submission
        """
        return self.do_once('SUBMIT', name)

    def rget(self, name):
        """
        Creates a unique-by-name command representing a recursive resource fetch
        """
        return self.do_once('RGET', name)

    def get(self, name):
        """
        Creates a unique-by-name command representing the fetch of a single
        resource
        """
        return self.do_once('GET', name)

    def stat(self, name):
        """
        Creates a unique-by-name command representing the fetch of a task's
        metadata
        """
        return self.do_once('STAT', name)

    def req(self, *commands):
        """
        Propagate result on failure
        """

        for command in commands:
            if command.is_failed():
                self.result = command.result
                break

        return status_conj(commands)

    @property
    def _str_res(self):
        return (
            f' = [{", ".join(str(r) for r in self.result)}]'
            if self.is_done() else ''
            )

class Script(Command):
    """
    A collection of maker methods for Commands
    """
    def __init__(self):
        super().__init__(self)
        self.commands = {}
        self.new_commands = deque()

    def collect(self, commands, allow_failures=False):
        """
        Creates a command representing a collection of other commands
        """
        return Collect(self, commands, allow_failures)

    def callback(self, command, callback):
        """
        Adds an arbitrary callback to an existing command
        """
        return Callback(command, callback)

    def update(self, *_, **_k):
        pass

    def __str__(self):
        return 'script'

@once
class Get(Command):
    """
    Get a single resource part
    """
    def __init__(self, script, name):
        self.name = name
        super().__init__(script)

    def __str__(self):
        return f' get {self.name}' + self._str_res

    def _eval(self):
        return Status.PENDING

@once
class Rget(Command):
    """
    Recursively gets a resource and all its parts
    """
    def __init__(self, script, name):
        self.name = name
        super().__init__(script)

    def __str__(self):
        return f'rget {self.name}'

    def _eval(self):
        """
        Eval the trigger condition on the given concrete values
        """
        get = self.get(self.name)

        sta = self.req(get)
        if sta:
            return sta

        return self.req(*[
            self.rget(child_name)
            for child_name in get.result
            ])

class Collect(Command):
    """
    A collection of other commands
    """
    def __init__(self, script, commands, allow_failures):
        self.commands = commands
        self.allow_failures = allow_failures
        for cmd in commands:
            cmd.out(self)
        super().__init__(script)

    def __str__(self):
        return 'collect'

    def _eval(self):
        if self.allow_failures:
            return status_conj_any(self.commands)

        return self.req(*self.commands)

class Callback(Command):
    """
    An arbitrary callback function, used to tie in other code
    """
    def __init__(self, command, callback):
        self._callback = callback
        self._in = command
        self._in.out(self)
        super().__init__(script=None)

    def __str__(self):
        return f'callback {self._callback.__name__}'

    def _eval(self):
        if self._in.status != Status.PENDING:
            self._callback(self._in.status)
            return Status.DONE
        return Status.PENDING

@once
class Rsubmit(Command):
    """
    A task submission
    """
    def __init__(self, script, name):
        self.name = name
        super().__init__(script)

    def __str__(self):
        return f'rsubmit {self.name}'

    def _eval(self):
        # metadata
        stat = self.stat(self.name)
        sta = self.req(stat)
        if sta:
            return sta

        task_done, stat_result = stat.result

        if not task_done:
            task_dict = stat_result

            # dependencies
            sta = self.req(*[
                self.rsubmit(dep)
                for dep in task_deps(task_dict)
                ])
            if sta:
                return sta

            # task itself
            main_sub = self.submit(self.name)
            sta = self.req(main_sub)
            if sta:
                return sta

            children = main_sub.result
        else:
            children = stat_result

        # Recursive children tasks
        return self.req(*[
            self.rsubmit(child_name)
            for child_name in children
            ])

@once
class Run(Command):
    """
    Combined rsubmit + rget
    """
    def __init__(self, script, name):
        self.name = name
        super().__init__(script)

    def __str__(self):
        return f'run {self.name}'

    def _eval(self):
        rsub = self.rsubmit(self.name)

        sta = self.req(rsub)
        if sta:
            return sta

        rget = self.rget(self.name)

        return self.req(rget)

@once
class Submit(Command):
    """
    Get a single resource part
    """
    def __init__(self, script, name):
        self.name = name
        super().__init__(script)

    def __str__(self):
        return f' submit {self.name}' + self._str_res

    def _eval(self):
        return Status.PENDING

@once
class Stat(Command):
    """
    Get a task's metadata
    """
    def __init__(self, script, name):
        self.name = name
        super().__init__(script)

    def __str__(self):
        return f' stat {self.name}' + self._str_res

    def _eval(self):
        return Status.PENDING
