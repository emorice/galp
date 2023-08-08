"""
Lists of internal commands
"""

import sys
import time
from collections import deque
from weakref import WeakSet, WeakValueDictionary
from typing import TypeVar, Generic, Callable, Any, Iterable, TypeAlias
from dataclasses import dataclass
from itertools import chain

from .task_types import (
        TaskName, LiteralTaskDef, QueryTaskDef, TaskDef, TaskOp, CoreTaskDef
        )

# Result types
# ============

Ok = TypeVar('Ok')
Err = TypeVar('Err')

@dataclass(frozen=True)
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

U = TypeVar('U')

# Helper typevars when a second ok type is needed
InOk = TypeVar('InOk')
OutOk = TypeVar('OutOk')

@dataclass
class Deferred(Generic[InOk, Ok, Err]):
    """Wraps commands with matching callback"""
    # Gotcha: the order of typevars differs from CallbackT !
    callback: 'PlainCallbackT[InOk, Err, Ok]'
    arg: 'Command[InOk, Err]'

    def then(self, callback_fn: 'CallbackT[Ok, OutOk, Err]') -> 'Deferred[Ok, OutOk, Err]':
        """
        Chain several callbacks
        """
        return DeferredCommand(self).then(callback_fn)

    def __repr__(self):
        return f'Deferred({self.callback.__qualname__})'

class Command(Generic[Ok, Err]):
    """
    Base class for commands
    """
    def __init__(self) -> None:
        self.val: Result[Ok, Err] = Pending()
        self.outputs : WeakSet[Command] = WeakSet()

    def __repr__(self):
        return f'Command(val={repr(self.val)})'

    def then(self, callback_fn: 'CallbackT[Ok, OutOk, Err]') -> Deferred[Ok, OutOk, Err]:
        """
        Chain callback to this command
        """
        return Deferred(ok_callback(callback_fn), self)

    def advance(self, script: 'Script'):
        """
        Try to advance the commands state. If the move causes new commands to be
        created or moves to a terminal state and trigger downstream commands,
        returns them for them to be advanced by the caller too.
        """
        old_value = self.val

        new_sub_commands = self._eval(script)

        script.notify_change(self, old_value, self.val)

        return new_sub_commands

    def _eval(self, script: 'Script') -> 'list[Command]':
        """
        Logic of calculating the value
        """
        raise NotImplementedError

    def is_pending(self):
        """
        Boolean, if command still pending
        """
        return isinstance(self.val, Pending)

    @property
    def inputs(self):
        """
        Commands we depend on
        """
        raise NotImplementedError

CallbackRet: TypeAlias = (
        Ok | Done[Ok] | Failed[Err] | Command[Ok, Err]
        | Deferred[Any, Err, Ok]
        )
CallbackT: TypeAlias = Callable[[InOk], CallbackRet[Ok, Err]]
PlainCallbackT: TypeAlias = Callable[[Done[InOk] | Failed[Err]], CallbackRet[Ok, Err]]

def ok_callback(callback_fn: CallbackT[InOk, Ok, Err]
        ) -> PlainCallbackT[InOk, Err, Ok]:
    """
    Wrap a callback from Ok values to accept and propagate Done/Failed
    accordingly
    """
    def _ok_callback(val: Done[InOk] | Failed[Err]):
        match val:
            case Done():
                # No need to wrap into Done since that's the default behavior
                return callback_fn(val.result)
            case Failed():
                return val
            case _:
                raise TypeError(val)
    return _ok_callback

class DeferredCommand(Command[Ok, Err]):
    """
    A promise of the return value Ok of a callback applied to an input promise
    InOk
    """

    def __init__(self, deferred: Deferred[Any, Ok, Err]) -> None:
        super().__init__()
        self._state = deferred
        deferred.arg.outputs.add(self)

    def __repr__(self):
        return f'Command(val={repr(self.val)}, state={repr(self._state)})'

    def _eval(self, script: 'Script') -> list[Command]:
        """
        State-based handling
        """
        sub_command = self._state.arg
        new_sub_commands = []
        value = sub_command.val
        while not isinstance(value, Pending): # = advance until stuck again

            # At this point, aggregate value is Done or Failed
            match self._state:
                case Deferred():
                    ret = self._state.callback(value)
            match ret:
                case Deferred():
                    self._state = ret
                case Command():
                    self._state = Deferred(lambda r: r, ret)
                case Failed() | Done():
                    self.val = ret
                    return []
                case _:
                    self.val = Done(ret)
                    return []

            sub_command = self._state.arg
            new_sub_commands = [self._state.arg]

            # Re-inspect values of new set of sub-commands and loop
            value = sub_command.val

        # Ensure all remaining sub-commands have downstream links pointing to
        # this command before relinquishing control
        sub_command.outputs.add(self)

        return new_sub_commands

    @property
    def inputs(self) -> list[Command]:
        """
        Commands we depend on
        """
        return [self._state.arg]

def as_command(thenable: 'Thenable[Ok, Err]') -> Command[Ok, Err]:
    """
    Wrap object in command if necessary
    """
    match thenable:
        case Command():
            return thenable
        case Deferred():
            return DeferredCommand(thenable)
        case _:
            raise TypeError(thenable)

class Gather(Command[list[Ok], Err]):
    """
    Then-able list
    """
    commands: list[Command[Ok, Err]]

    def __init__(self, commands: 'Thenable[Ok, Err]' | Iterable['Thenable[Ok, Err]']):
        super().__init__()
        thenables = commands if isinstance(commands, Iterable) else [commands]
        self.commands = list(map(as_command, thenables))
        for inp in self.commands:
            inp.outputs.add(self)

    def _eval(self, script: 'Script'):
        """
        State-based handling
        """
        self.val = script.value_conj(self.commands)
        return []

    @property
    def inputs(self):
        """
        Commands we depend on
        """
        return self.commands

Thenable: TypeAlias = Command[Ok, Err] | Deferred[Any, Ok, Err]

class InertCommand(Command[Ok, Err]):
    """
    Command tied to an external event

    Fully defined and identified by a task name
    """
    # Strong ref to master. This allows to make the master dict weak, which in
    # turns drops the master when the last copy gets collected
    # This is purely for memory management and should not be used for any other
    # purpose
    master: 'PrimitiveProxy'

    def __init__(self, name: TaskName):
        self.name = name
        super().__init__()

    def __str__(self):
        return f'{self.__class__.__name__.lower()} {self.name}'

    def _eval(self, *_):
        raise NotImplementedError

    @property
    def key(self) -> tuple[str, TaskName]:
        """
        Unique key
        """
        return (self.__class__.__name__.upper(), self.name)

    @property
    def inputs(self):
        """
        Commands we depend on
        """
        return []

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

class Script:
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

    def collect(self, commands: list[Command[Ok, Err]]) -> Command[list[Ok], Err]:
        """
        Creates a command representing a collection of other commands
        """
        return Gather(commands)

    def callback(self, thenable: Thenable[InOk, Err],
            callback_fn: PlainCallbackT[InOk, Err, Ok]) -> Command[Ok, Err]:
        """
        Adds an arbitrary callback to an existing command. This triggers
        execution of the command as far as possible, and of the callback if
        ready.
        """
        command = as_command(thenable)
        cb_command = callback(command, callback_fn)
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
    commands = get_all_subcommands(commands)
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
                    sub_commands = get_all_subcommands(sub_commands)
                    commands.extend(sub_commands)
                # Maybe this caused the command to advance to a terminal state (DONE or
                # FAILED). In that case downstream commands must be checked too.
                elif not command.is_pending():
                    commands.extend(command.outputs)

    # Compat with previous, non-functional interface
    script.new_commands.extend(p.key for p in primitives)
    return primitives

def get_all_subcommands(commands):
    """
    Collect all the nodes of a command tree
    """
    all_commands = []
    commands = list(commands)
    while commands:
        cmd = commands.pop()
        all_commands.append(cmd)
        commands.extend(cmd.inputs)
    return all_commands

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

def rget(name: TaskName) -> Deferred[Any, Any, str]:
    """
    Get a task result, then rescursively get all the sub-parts of it
    """
    return (
        Get(name)
        .then(lambda children: Gather(map(rget, children)))
        )

def callback(command: Command[InOk, Err],
        callback_fn: PlainCallbackT[InOk, Err, Ok]) -> Command[Ok, Err]:
    """
    Glue for old Callback command
    """
    return DeferredCommand(Deferred(callback_fn, command))

def ssubmit(name: TaskName, dry: bool = False
            ) -> Deferred[Any, list[TaskName], str]:
    """
    A non-recursive ("simple") task submission: executes dependencies, but not
    children. Return said children as result on success.
    """
    return Stat(name).then(lambda statr: _ssubmit(statr, dry))

def _ssubmit(stat_result: StatResult, dry: bool
             ) -> list[TaskName] | Deferred[Any, list[TaskName], str]:
    """
    Core ssubmit logic, recurse on dependencies and skip done tasks
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
    # FIXME: optimize type of command based on type of link
    # Always doing a RSUB will work, but it will run things more eagerly that
    # needed or propagate failures too aggressively.
    return (
            Gather([rsubmit(tin.name,dry) for tin in task_def.dependencies(TaskOp.BASE)])
            .then(lambda _: [] if dry else Submit(task_def.name))
            )

def rsubmit(name: TaskName, dry: bool = False):
    """
    Recursive submit, with children, i.e a ssubmit plus a rsubmit per child

    For dry-runs, does not actually submit tasks, and instead moves on as soon
    as the STAT command succeeded,

    If a task has dynamic children (i.e., running the task returns new tasks to
    execute), the output will depend on the task status:
     * If the task is done, the dry-run will be able to list the child tasks and
       recurse on them.
     * If the task is not done, the dry-run will not see the child tasks and
       skip them, so only the parent task will show up in the dry-run log.

    This is the intended behavior, it reflects the inherent limit of a dry-run:
    part of the task graph is unknown before we actually start executing it.
    """
    return (
            ssubmit(name, dry)
            .then(lambda children: Gather([rsubmit(c, dry) for c in children]))
            )

def run(name: TaskName, dry=False):
    """
    Combined rsubmit + rget

    If dry, just a dry rsubmit
    """
    if dry:
        return rsubmit(name, dry=True)
    return rsubmit(name).then(lambda _: rget(name))

def srun(name: TaskName):
    """
    Shallow run: combined ssubmit + get, fetches the raw result of a task but
    not its children
    """
    return ssubmit(name).then(lambda _: Get(name))
