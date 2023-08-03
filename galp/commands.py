"""
Lists of internal commands
"""

import sys
import time
from enum import Enum
from collections import deque
from weakref import WeakSet, WeakValueDictionary
from typing import TypeVar, Generic
from dataclasses import dataclass
from itertools import chain

import logging

from .task_types import (
        TaskName, LiteralTaskDef, QueryTaskDef, TaskDef, TaskOp
        )

class Status(Enum):
    """
    Command status
    """
    PENDING = 'P'
    DONE = 'D'
    FAILED = 'F'

    def __bool__(self):
        return self != Status.DONE

T = TypeVar('T')

class Command(Generic[T]):
    """
    An asynchronous command
    """
    def __init__(self) -> None:
        self.status = Status.PENDING
        self.result: T = None
        self.outputs : WeakSet[Command] = WeakSet()
        self._state = '_init'
        self._sub_commands : list[Command] = []

    def _eval(self, script: 'Script'):
        """
        State-based handling
        """
        sub_commands = self._sub_commands
        new_sub_commands = []
        # Aggregate status of all sub-commands
        sub_status = script.status_conj(sub_commands)
        while sub_status != Status.PENDING:
            # By default, we stop processing a command on failures. In theory
            # the command could have other independent work to do, but we have
            # no use case yet.
            # Note: this drops new_sub_commands by design since there is no need
            # to inject them into the event loop with the parent failed.
            if sub_status == Status.FAILED:
                self.result = next(
                        sub.result
                        for sub in sub_commands
                        if sub.status == Status.FAILED)
                return Status.FAILED, []

            # At this point, all sub commands are DONE

            try:
                handler = getattr(self, self._state)
            except KeyError:
                raise NotImplementedError(self.__class__, self._state) from None

            results = [sub.result for sub in sub_commands]
            ret = handler(*results)
            # Sugar: allow omitting sub commands or passing only one instead of
            # a list
            if isinstance(ret, tuple):
                new_state, sub_commands = ret
            else:
                new_state, sub_commands = ret, []
            if isinstance(sub_commands, Command):
                sub_commands = [sub_commands]

            new_sub_commands = sub_commands

            logging.debug('%s: %s => %s', self, self._state, new_state)
            self._state = new_state
            if self._state in (Status.DONE, Status.FAILED):
                assert not new_sub_commands
                return self._state, []

            # Re-inspect status of new set of sub-commands and loop
            sub_status = script.status_conj(sub_commands)

        # Ensure all remaining sub-commands have downstream links pointing to
        # this command before relinquishing control
        for sub in sub_commands:
            sub.outputs.add(self)

        # Save for callback
        self._sub_commands = sub_commands

        return Status.PENDING, new_sub_commands

    def advance(self, script: 'Script'):
        """
        Try to advance the commands state. If the move causes new commands to be
        created or moves to a terminal state and trigger downstream commands,
        returns them for them to be advanced by the caller too.
        """
        old_status = self.status

        self.status, new_sub_commands = self._eval(script)

        script.notify_change(self, old_status, self.status)

        logging.info('%s %s', self.status.name.ljust(7), self)

        return new_sub_commands

    def done(self, result=None):
        """
        Mark the future as done and runs callbacks.

        A task can be safely marked several times, the callbacks are run only
        when the status transitions from unknown to known.

        However successive done calls will overwrite the result.
        """
        return self._mark_as(Status.DONE, result)

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

    def _mark_as(self, status: Status, result: T) -> None:
        """
        Externally change the status of the command.
        """
        assert isinstance(self, InertCommand)

        # Guard against double marks
        # FIXME: This typically happen because we cannot tell apart STAT-DONE and
        # SUBMIT-DONE pairs, we should have distinct messages
        if self.status != Status.PENDING:
            return

        self.status = status
        self.result = result

        # Removed: this is now the caller's responsibility
        #advance_all(self.script, self.outputs)

    @property
    def _str_res(self):
        return (
            f' = [{", ".join(str(r) for r in self.result)}]'
            if self.status == Status.DONE else ''
            )

class UniqueCommand(Command[T]):
    """
    Any command that is fully defined and identified by a task name
    """
    def __init__(self, name: TaskName):
        self.name = name
        super().__init__()

    def __str__(self):
        return f'{self.__class__.__name__.lower()} {self.name}'

class InertCommand(UniqueCommand[T]):
    """
    Command tied to an external event
    """
    # Strong ref to master. This allows to make the master dict weak, which in
    # turns drops the master when the last copy gets collected
    # This is purely for memory management and should not be used for any other
    # purpose
    master: 'PrimitiveProxy'

    def __str__(self):
        return f'{self.__class__.__name__.lower().ljust(7)} {self.name}' + self._str_res

    def _eval(self, *_):
        raise NotImplementedError

    @property
    def key(self) -> tuple[str, TaskName]:
        """
        Unique key
        """
        return (self.__class__.__name__.upper(), self.name)

Ok = TypeVar('Ok')
Err = TypeVar('Err')

# pylint: disable=too-few-public-methods
class Pending():
    """
    Special type for a result not known yet
    """

@dataclass
class Done(Generic[Ok]):
    """
    Wrapper type for result
    """
    result: Ok

@dataclass
class Failed(Generic[Err]):
    """
    Wrapper type for error
    """
    error: Err

Result = Pending | Done[Ok] | Failed[Err]

class PrimitiveProxy(Generic[Ok, Err]):
    """
    Helper class to propagate results to several logical instances of the same
    primitive
    """
    instances: WeakSet[InertCommand]
    val: Result[Ok, Err] = Pending()
    script: 'Script'

    def __init__(self, script: 'Script', *instances: InertCommand):
        self.instances = WeakSet(instances)
        self.script = script

    def done(self, result: Ok) -> list[InertCommand]:
        """
        Mark all instances as done
        """
        self.val = Done(result)
        for cmd in self.instances:
            cmd.done(result)
        return self._advance_all()

    def failed(self, error: Err) -> list[InertCommand]:
        """
        Mark all instances as failed
        """
        self.val = Failed(error)
        for cmd in self.instances:
            cmd.failed(error)
        return self._advance_all()

    def _advance_all(self) -> list[InertCommand]:
        return advance_all(self.script,
                list(chain.from_iterable(cmd.outputs for cmd in self.instances))
                )

    def is_pending(self) -> bool:
        """
        Check if pending
        """
        return isinstance(self.val, Pending)

CommandKey = tuple[str, TaskName]

class Script(Command):
    """
    A collection of maker methods for Commands

    Args:
        verbose: whether to print brief summary of command completion to stderr
        store: Store object, destination to store Query result. Only needed for
            queries.
        keep_going: whether to attempt to complete independent commands when one
            fails. If False, attempts to "fail fast" instead, and finishes as
            soon as any command fails.
    """
    def __init__(self, verbose=True, store=None, keep_going: bool = False):
        # Strong references to all pending commands
        self.pending: set[Command] = set()

        super().__init__()
        self.commands : WeakValueDictionary[CommandKey, PrimitiveProxy] = WeakValueDictionary()
        self.new_commands : deque[CommandKey] = deque()
        self.verbose = verbose
        self.keep_going = keep_going
        self.store = store

        self._task_dicts = {}

    def collect(self, commands):
        """
        Creates a command representing a collection of other commands
        """
        cmd =  Collect(commands)
        return cmd

    def callback(self, command: Command, callback):
        """
        Adds an arbitrary callback to an existing command. This triggers
        execution of the command as far as possible, and of the callback if
        ready.
        """
        cb_command = Callback(command, callback)
        self.pending.add(cb_command)
        # The callback is set as a downstream link to the command, but it still
        # need an external trigger, so we need to advance it
        advance_all(self, [command])
        return cb_command

    def notify_change(self, command: Command, old_status: Status, new_status: Status):
        """
        Hook called when the graph status changes
        """
        # Dereference non-pending commands, thus allowing deletion if nobody else
        # keeps a strong reference to the command.
        # This is intended so that commands triggering a callback stay in memory
        # until they're final, then get collected once the callback has fired.
        if command in self.pending and new_status in (Status.DONE,
                Status.FAILED):
            self.pending.remove(command)

        if not self.verbose:
            return
        del old_status

        def print_status(stat, name, step_name):
            print(f'{time.strftime("%Y-%m-%d %H:%M:%S")} {stat:4} [{name}]'
                    f' {step_name}',
                    file=sys.stderr, flush=True)

        if isinstance(command, Stat):
            if new_status == Status.DONE:
                task_done, task_dict, _children = command.result
                if not task_done and 'step_name' in task_dict:
                    self._task_dicts[command.name] = task_dict
                    print_status('PLAN', command.name,
                            task_dict['step_name'].decode('ascii'))

        if isinstance(command, Submit):
            if command.name in self._task_dicts:
                step_name_s = self._task_dicts[command.name]['step_name'].decode('ascii')

                # We log 'SUB' every time a submit command's status "changes" to
                # PENDING. Currently this only happens when the command is
                # created.
                if new_status == Status.PENDING:
                    print_status('SUB', command.name, step_name_s)
                elif new_status == Status.DONE:
                    print_status('DONE', command.name, step_name_s)
                if new_status == Status.FAILED:
                    print_status('FAIL', command.name, step_name_s)

    def advance(self, *_, **_k):
        return []

    def __str__(self):
        return 'script'

    def status_conj(self, commands):
        """
        Status conjunction respecting self.keep_going
        """
        status = [cmd.status for cmd in commands]

        if all(s == Status.DONE for s in status):
            return Status.DONE

        if self.keep_going:
            if any(s == Status.PENDING for s in status):
                return Status.PENDING
            return Status.FAILED

        if any(s == Status.FAILED for s in status):
            return Status.FAILED
        return Status.PENDING

def advance_all(script: Script, commands: list[Command]) -> list[InertCommand]:
    """
    Try to advance all given commands, and all downstream depending on them the
    case being
    """
    commands = list(commands)
    primitives : list[InertCommand] = []

    while commands:
        command = commands.pop()
        match command:
            case InertCommand():
                # Check if we already have instances of that command
                master = script.commands.get(command.key)
                if master is None:
                    # If not create it, and add it to the new primitives to return
                    master = PrimitiveProxy(script, command)
                    command.master = master # strong
                    script.commands[command.key] = master # weak
                    primitives.append(command)
                else:
                    # If yes, depends if it's resolved
                    match master.val:
                        case Pending():
                            # If not, we simply add our copy to the list
                            master.instances.add(command) # weak
                            command.master = master # strong
                            continue
                            # If yes, transfer the state and schedule the rest
                        case Done():
                            command.done(master.val.result)
                        case Failed():
                            command.failed(master.val.error)
                    commands.extend(command.outputs)
            case _:
                if command.status != Status.PENDING:
                    # FIXME: Why does that even happen ?
                    continue
                # For now, subcommands need to be recursively initialized
                sub_commands = command.advance(script)
                if sub_commands:
                    commands.extend(sub_commands)
                # Maybe this caused the command to advance to a terminal state (DONE or
                # FAILED). In that case downstream commands must be checked too.
                elif command.status != Status.PENDING:
                    commands.extend(command.outputs)

    # Compat with previous, non-functional interface
    script.new_commands.extend(p.key for p in primitives)
    return primitives

class Get(InertCommand[list[TaskName]]):
    """
    Get a single resource part
    """

class Submit(InertCommand[list[TaskName]]):
    """
    Remotely execute a single step
    """

# done, def, children
StatResult = tuple[bool, TaskDef, list[TaskName]]

class Stat(InertCommand[StatResult]):
    """
    Get a task's metadata
    """

class Rget(UniqueCommand[None]):
    """
    Recursively gets a resource and all its parts
    """
    def _init(self):
        return '_get', Get(self.name)

    def _get(self, children: list[TaskName]):
        return '_sub_gets', [ Rget(child_name) for child_name in children ]

    def _sub_gets(self, *_):
        return Status.DONE

class Collect(Command[list]):
    """
    A collection of other commands
    """
    def __init__(self, commands):
        self.commands = commands
        super().__init__()

    def __str__(self):
        return 'collect'

    def _init(self):
        return '_collect', self.commands

    def _collect(self, *results):
        self.result = results
        return Status.DONE

class Callback(Command):
    """
    An arbitrary callback function, used to tie in other code

    It is important that a callback command is attached to a script, else it can
    be collected before having a chance to fire since downstream links are
    weakrefs.
    """
    def __init__(self, command, callback):
        self._callback = callback
        self._in = command
        self._in.outputs.add(self)
        super().__init__()

    def __str__(self):
        return f'callback {self._callback.__name__}'

    def _eval(self, script: 'Script'):
        if self._in.status != Status.PENDING:
            self._callback(self._in.status, self._in.result)
            return Status.DONE, []
        return Status.PENDING, []

class SSubmit(UniqueCommand[list[TaskName]]):
    """
    A non-recursive ("simple") task submission: executes dependencies, but not
    children. Return said children as result on success.
    """
    _dry = False

    def _init(self):
        """
        Emit the initial stat
        """
        # metadata
        return '_sort', Stat(self.name)

    def _sort(self, stat_result: StatResult):
        """
        Sort out queries, remote jobs and literals
        """
        _task_done, task_def, children = stat_result

        # Short circuit for tasks already processed and literals
        if isinstance(task_def, LiteralTaskDef):
            children = task_def.children

        if children is not None:
            self.result = children
            return Status.DONE

        # Query, should never have reached this layer as queries have custom run
        # mechanics
        if isinstance(task_def, QueryTaskDef):
            raise NotImplementedError

        # Else, regular job, process dependencies first
        cmd = DryRun if self._dry else RSubmit
        # FIXME: optimize type of command based on type of link
        # Always doing a RSUB will work, but it will run things more eagerly that
        # needed or propagate failures too aggressively.
        return '_deps', [
                cmd(tin.name)
                for tin in task_def.dependencies(TaskOp.BASE)
                ]

    def _deps(self, *_):
        """
        Check deps availability before proceeding to main submit
        """
        # For dry-runs, bypass the submission
        if self._dry:
            # Explicitely set the result to "no children"
            self.result = []
            return Status.DONE

        # Task itself
        return '_main', Submit(self.name)

    def _main(self, children: list[TaskName]) -> Status:
        """
        Check the main result and return possible children tasks
        """
        self.result = children
        return Status.DONE

class DrySSubmit(SSubmit):
    """
    Dry-run variant of SSubmit
    """
    _dry = True

class RSubmit(UniqueCommand[None]):
    """
    Recursive submit, with children, aka an SSubmit plus a RSubmit per child
    """
    _dry = False

    def _init(self):
        """
        Emit the simple submit
        """
        cmd = DrySSubmit if self._dry else SSubmit
        return '_main', cmd(self.name)

    def _main(self, children: list[TaskName]):
        """
        Check the main result and proceed with possible children tasks
        """
        cmd = DryRun if self._dry else RSubmit
        return '_children', [ cmd(child_name)
                for child_name in children ]

    def _children(self, *_):
        """
        Check completion of children and propagate result
        """
        return Status.DONE

# Despite the name, since a DryRun never does any fetches, it is implemented as
# a special case of RSubmit and not Run (Run is RSubmit + RGet)
class DryRun(RSubmit):
    """
    A task dry-run. Similar to RSUBMIT, but does not actually submit tasks, and
    instead moves on as soon as the STAT command succeeded.

    If a task has dynamic children (i.e., running the task returns new tasks to
    execute), the output will depend on the task status:
     * If the task is done, the dry-run will be able to list the child tasks and
       recurse on them.
     * If the task is not done, the dry-run will not see the child tasks and
       skip them, so only the parent task will show up in the dry-run log.

    This is the intended behavior, it reflects the inherent limit of a dry-run:
    part of the task graph is unknown before we actually start executing it.
    """
    _dry = True

class Run(UniqueCommand[None]):
    """
    Combined rsubmit + rget
    """
    def _init(self):
        return '_rsub', RSubmit(self.name)

    def _rsub(self, _):
        return '_rget', Rget(self.name)

    def _rget(self, _):
        return Status.DONE

class SRun(UniqueCommand[None]):
    """
    Shallow run: combined ssubmit + get, fetches the raw result of a task but
    not its children
    """
    def _init(self):
        return '_ssub', SSubmit(self.name)

    def _ssub(self, _):
        return '_get', Get(self.name)

    def _get(self, _):
        return Status.DONE
