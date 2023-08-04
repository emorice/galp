"""
Lists of internal commands
"""

import sys
import time
from collections import deque
from weakref import WeakSet, WeakValueDictionary
from typing import TypeVar, Generic, Callable, Any, Protocol
from dataclasses import dataclass
from itertools import chain

import logging

from .task_types import (
        TaskName, LiteralTaskDef, QueryTaskDef, TaskDef, TaskOp, CoreTaskDef
        )

# Result types
# ============

Ok = TypeVar('Ok')
Err = TypeVar('Err')

# pylint: disable=too-few-public-methods
class Pending():
    """
    Special type for a result not known yet
    """

@dataclass(frozen=True)
class Done(Generic[Ok]):
    """
    Wrapper type for result
    """
    result: Ok

@dataclass(frozen=True)
class Failed(Generic[Err]):
    """
    Wrapper type for error
    """
    error: Err

Result = Pending | Done[Ok] | Failed[Err]
FinalResult = Done[Ok] | Failed[Err]

# Commands
# ========

T_contra = TypeVar('T_contra', contravariant=True)
U = TypeVar('U')
class CallbackT(Protocol[T_contra, U]):
    """Callback type"""
    def __call__(self, *args: T_contra) -> 'CallbackRet[Any, U, Any] | U': ...

InOk = TypeVar('InOk')

@dataclass
class Deferred(Generic[InOk, Ok, Err]):
    """Wraps commands with matching callback"""
    callback: CallbackT[InOk, Ok]
    inputs: list['Command[InOk, Err]']

class Command(Generic[Ok, Err]):
    """
    A promise of the return value Ok of a callback applied to an input promise
    InOk
    """

    def __init__(self) -> None:
        super().__init__()
        self.val: Result[Ok, Err] = Pending()
        self.outputs : WeakSet[Command] = WeakSet()
        self._state: Deferred[Any, Ok, Err]= Deferred(self._init, [])

    def _init(self, *_):
        raise NotImplementedError

    def _end(self, *_):
        raise RuntimeError('Dont call me')

    def _eval(self, script: 'Script'):
        """
        State-based handling
        """
        sub_commands = self._state.inputs
        new_sub_commands = []
        # Aggregate value of all sub-commands
        agg_value = script.value_conj(sub_commands)
        while not isinstance(agg_value, Pending): # = advance until stuck again
            # Note: this drops new_sub_commands by design since there is no need
            # to inject them into the event loop with the parent failed.
            if isinstance(agg_value, Failed):
                self.val = agg_value
                return []

            # At this point, aggregate value is Done

            ret = self._state.callback(*agg_value.result)
            match ret:
                case Deferred():
                    self._state = ret
                case Failed() | Done():
                    self.val = ret
                    return []
                case _:
                    self.val = Done(ret)
                    return []

            sub_commands = self._state.inputs
            new_sub_commands = self._state.inputs

            # Re-inspect values of new set of sub-commands and loop
            agg_value = script.value_conj(sub_commands)

        # Ensure all remaining sub-commands have downstream links pointing to
        # this command before relinquishing control
        for sub in sub_commands:
            sub.outputs.add(self)

        return new_sub_commands

    def then(self, callback: CallbackT[Ok, U]) -> Deferred[Ok, U, Err]:
        """
        Chain callback to this command
        """
        return Deferred(callback, [self])

    def advance(self, script: 'Script'):
        """
        Try to advance the commands state. If the move causes new commands to be
        created or moves to a terminal state and trigger downstream commands,
        returns them for them to be advanced by the caller too.
        """
        old_value = self.val

        new_sub_commands = self._eval(script)

        script.notify_change(self, old_value, self.val)
        logging.info('%s %s', self.val, self)

        return new_sub_commands

    def is_pending(self):
        """
        Boolean, if command still pending
        """
        return isinstance(self.val, Pending)

    @property
    def _str_res(self) -> str:
        return (
            f' = [{", ".join(str(r) for r in self.val.result)}]'
            if isinstance(self.val, Done) else ''
            )

CallbackRet = (
        tuple[str, list[Command[InOk, Err]] | Command[InOk, Err]] | str
        | Deferred[InOk, Ok, Err]
        )

class Gather(Generic[Ok, Err]):
    """
    Then-able list
    """
    commands: list[Command[Ok, Err]]

    def __init__(self, commands: Command | list[Command]):
        if isinstance(commands, Command):
            self.commands = [commands]
        else:
            self.commands = commands

    def then(self, callback: CallbackT[Ok, U]) -> Deferred[Ok, U, Err]:
        """
        Chain callback to this list
        """
        return Deferred(callback, self.commands)

class UniqueCommand(Command[Ok, Err]):
    """
    Any command that is fully defined and identified by a task name
    """
    def __init__(self, name: TaskName):
        self.name = name
        super().__init__()

    def __str__(self):
        return f'{self.__class__.__name__.lower()} {self.name}'

class InertCommand(UniqueCommand[Ok, Err]):
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
        return self._advance_all()

    def failed(self, error: Err) -> list[InertCommand]:
        """
        Mark all instances as failed
        """
        self.val = Failed(error)
        return self._advance_all()

    def _advance_all(self) -> list[InertCommand]:
        for cmd in self.instances:
            cmd.val = self.val
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

        # For logging
        self._task_defs: dict[TaskName, CoreTaskDef] = {}

    def collect(self, commands: list[Command]) -> 'Collect':
        """
        Creates a command representing a collection of other commands
        """
        return Collect(commands)

    def callback(self, command: Command, callback: Callable[[FinalResult], Any]):
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

    def notify_change(self, command: Command, old_value: Result, new_value: Result):
        """
        Hook called when the graph status changes
        """
        # Dereference non-pending commands, thus allowing deletion if nobody else
        # keeps a strong reference to the command.
        # This is intended so that commands triggering a callback stay in memory
        # until they're final, then get collected once the callback has fired.
        if command in self.pending and not isinstance(self, Pending):
            self.pending.remove(command)

        if not self.verbose:
            return
        del old_value

        def print_status(stat, name, step_name):
            print(f'{time.strftime("%Y-%m-%d %H:%M:%S")} {stat:4} [{name}]'
                    f' {step_name}',
                    file=sys.stderr, flush=True)

        if isinstance(command, Stat):
            if isinstance(new_value, Done):
                task_done, task_def, _children = new_value.result
                if not task_done and isinstance(self, CoreTaskDef):
                    self._task_defs[command.name] = task_def
                    print_status('PLAN', command.name, task_def.step)

        if isinstance(command, Submit):
            if command.name in self._task_defs:
                step_name_s = self._task_defs[command.name].step

                # We log 'SUB' every time a submit command's status "changes" to
                # PENDING. Currently this only happens when the command is
                # created.
                match new_value:
                    case Pending():
                        print_status('SUB', command.name, step_name_s)
                    case Done():
                        print_status('DONE', command.name, step_name_s)
                    case Failed():
                        print_status('FAIL', command.name, step_name_s)

    def advance(self, *_, **_k):
        return []

    def __str__(self):
        return 'script'

    def value_conj(self, commands: list[Command[Ok, Err]]
                    ) -> Result[list[Ok], Err]:
        """
        Value conjunction respecting self.keep_going
        """
        results = []
        failed = None
        for val in (cmd.val for cmd in commands):
            match val:
                case Pending():
                    return Pending()
                case Done():
                    results.append(val.result)
                case Failed():
                    if not self.keep_going:
                        return Failed(val.error)
                    if failed is None:
                        failed = val
        if failed is None:
            return Done(results)
        return Failed(failed.error)

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
                    if master.is_pending():
                        # If not, we simply add our copy to the list
                        master.instances.add(command) # weak
                        command.master = master # strong
                    else:
                        # If yes, transfer the state and schedule the rest
                        command.val = master.val
                        commands.extend(command.outputs)
            case _:
                if not command.is_pending():
                    # FIXME: Why does that even happen ?
                    continue
                # For now, subcommands need to be recursively initialized
                sub_commands = command.advance(script)
                if sub_commands:
                    commands.extend(sub_commands)
                # Maybe this caused the command to advance to a terminal state (DONE or
                # FAILED). In that case downstream commands must be checked too.
                elif not command.is_pending():
                    commands.extend(command.outputs)

    # Compat with previous, non-functional interface
    script.new_commands.extend(p.key for p in primitives)
    return primitives

class Get(InertCommand[list[TaskName], str]):
    """
    Get a single resource part
    """

class Submit(InertCommand[list[TaskName], str]):
    """
    Remotely execute a single step
    """

# done, def, children
StatResult = tuple[bool, TaskDef, list[TaskName]]

class Stat(InertCommand[StatResult, str]):
    """
    Get a task's metadata
    """

class Rget(UniqueCommand[None, str]):
    """
    Recursively gets a resource and all its parts
    """
    def _init(self) -> Deferred:
        return Get(self.name).then(self._get)

    def _get(self, children: list[TaskName]):
        return Gather([
            Rget(child_name) for child_name in children
            ]).then(self._sub_gets)

    def _sub_gets(self, *_):
        return

class Collect(Command[list, str]):
    """
    A collection of other commands
    """
    def __init__(self, commands):
        self.commands = commands
        super().__init__()

    def __str__(self):
        return 'collect'

    def _init(self):
        return Gather(self.commands).then(self._collect)

    def _collect(self, *results):
        return results

class Callback(Command):
    """
    An arbitrary callback function, used to tie in other code

    It is important that a callback command is attached to a script, else it can
    be collected before having a chance to fire since downstream links are
    weakrefs.
    """
    def __init__(self, command: Command, callback: Callable[[FinalResult], Any]):
        self._callback = callback
        self._in = command
        self._in.outputs.add(self)
        super().__init__()

    def __str__(self):
        return f'callback {self._callback.__name__}'

    def _eval(self, script: 'Script'):
        in_val = self._in.val
        if not isinstance(in_val, Pending):
            self._callback(in_val)
        return []

class SSubmit(UniqueCommand[list[TaskName], str]):
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
        return Stat(self.name).then(self._sort)

    def _sort(self, stat_result: StatResult):
        """
        Sort out queries, remote jobs and literals
        """
        _task_done, task_def, children = stat_result

        # Short circuit for tasks already processed and literals
        if isinstance(task_def, LiteralTaskDef):
            children = task_def.children

        if children is not None:
            return children

        # Query, should never have reached this layer as queries have custom run
        # mechanics
        if isinstance(task_def, QueryTaskDef):
            raise NotImplementedError

        # Else, regular job, process dependencies first
        cmd = DryRun if self._dry else RSubmit
        # FIXME: optimize type of command based on type of link
        # Always doing a RSUB will work, but it will run things more eagerly that
        # needed or propagate failures too aggressively.
        return Gather([
                cmd(tin.name)
                for tin in task_def.dependencies(TaskOp.BASE)
                ]).then(self._deps)

    def _deps(self, *_):
        """
        Check deps availability before proceeding to main submit
        """
        # For dry-runs, bypass the submission
        if self._dry:
            # Explicitely set the result to "no children"
            return []

        # Task itself
        return Submit(self.name).then(self._main)

    def _main(self, children: list[TaskName]):
        """
        Check the main result and return possible children tasks
        """
        return children

class DrySSubmit(SSubmit):
    """
    Dry-run variant of SSubmit
    """
    _dry = True

class RSubmit(UniqueCommand[None, str]):
    """
    Recursive submit, with children, aka an SSubmit plus a RSubmit per child
    """
    _dry = False

    def _init(self):
        """
        Emit the simple submit
        """
        cmd = DrySSubmit if self._dry else SSubmit
        return cmd(self.name).then(self._main)

    def _main(self, children: list[TaskName]):
        """
        Check the main result and proceed with possible children tasks
        """
        cmd = DryRun if self._dry else RSubmit
        return Gather([ cmd(child_name)
                for child_name in children ]).then(self._children)

    def _children(self, *_):
        """
        Check completion of children and propagate result
        """
        return

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

class Run(UniqueCommand[None, str]):
    """
    Combined rsubmit + rget
    """
    def _init(self):
        return RSubmit(self.name).then(self._rsub)

    def _rsub(self, _):
        return Rget(self.name).then(self._rget)

    def _rget(self, _):
        return

class SRun(UniqueCommand[None, str]):
    """
    Shallow run: combined ssubmit + get, fetches the raw result of a task but
    not its children
    """
    def _init(self):
        return SSubmit(self.name).then(self._ssub)

    def _ssub(self, _):
        return Get(self.name).then(self._get)

    def _get(self, _):
        return
