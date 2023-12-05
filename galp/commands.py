"""
Lists of internal commands
"""

import sys
import time
import logging
from weakref import WeakSet, WeakValueDictionary
from typing import TypeVar, Generic, Callable, Any, Iterable, TypeAlias
from dataclasses import dataclass
from itertools import chain
from functools import wraps

import galp.messages as gm
import galp.task_types as gtt

# Result types
# ============

# pylint: disable=typevar-name-incorrect-variance
Ok = TypeVar('Ok', covariant=True)
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

# Helper typevars when a second ok type is needed
InOk = TypeVar('InOk')
OutOk = TypeVar('OutOk')

@dataclass
class Deferred(Generic[InOk, Ok, Err]):
    """Wraps commands with matching callback"""
    # Gotcha: the order of typevars differs from Callback !
    callback: 'PlainCallback[InOk, Err, Ok]'
    arg: 'Command[InOk, Err]'

    def __repr__(self):
        return f'Deferred({self.callback.__qualname__})'

class Command(Generic[Ok, Err]):
    """
    Base class for commands
    """
    def __init__(self) -> None:
        self.val: Result[Ok, Err] = Pending()
        self.outputs : WeakSet[Command] = WeakSet()

    def then(self, callback: 'Callback[Ok, OutOk, Err]') -> 'Command[OutOk, Err]':
        """
        Chain callback to this command
        """
        return DeferredCommand(Deferred(ok_callback(callback), self))

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
        )
Callback: TypeAlias = Callable[[InOk], CallbackRet[Ok, Err]]
PlainCallback: TypeAlias = Callable[[Done[InOk] | Failed[Err]], CallbackRet[Ok, Err]]

def ok_callback(callback: Callback[InOk, Ok, Err]
        ) -> PlainCallback[InOk, Err, Ok]:
    """
    Wrap a callback from Ok values to accept and propagate Done/Failed
    accordingly
    """
    @wraps(callback)
    def _ok_callback(val: Done[InOk] | Failed[Err]):
        match val:
            case Done():
                # No need to wrap into Done since that's the default behavior
                return callback(val.result)
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
            ret = self._state.callback(value)
            match ret:
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

class Gather(Command[list[Ok], Err]):
    """
    Then-able list
    """
    commands: list[Command[Ok, Err]]

    def __init__(self, commands: 'Command[Ok, Err]' | Iterable['Command[Ok, Err]']):
        super().__init__()
        self.commands = list(commands) if isinstance(commands, Iterable) else [commands]
        for inp in self.commands:
            inp.outputs.add(self)

    def _eval(self, script: 'Script'):
        """
        State-based handling
        """
        self.val = script.value_conj(self.commands)
        return []

    def __repr__(self):
        return f'Gather({repr(self.commands)} = {repr(self.val)})'

    @property
    def inputs(self):
        """
        Commands we depend on
        """
        return self.commands

CommandKey = tuple[str, gtt.TaskName]

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

    def _eval(self, *_):
        # Never changes by itself
        return []

    @property
    def key(self) -> CommandKey:
        """
        Unique key
        """
        raise NotImplementedError

    @property
    def inputs(self):
        """
        Commands we depend on
        """
        return []

class NamedPrimitive(InertCommand[Ok, Err]):
    """
    Primitive made of just a task name
    """
    def __init__(self, name: gtt.TaskName):
        self.name = name
        super().__init__()

    @property
    def key(self) -> CommandKey:
        """
        Unique key
        """
        return (self.__class__.__name__.upper(), self.name)

Ok_contra = TypeVar('Ok_contra', contravariant=True)

class PrimitiveProxy(Generic[Ok_contra, Err]):
    """
    Helper class to propagate results to several logical instances of the same
    primitive
    """
    instances: WeakSet[InertCommand]
    val: Result[Ok_contra, Err] = Pending()
    script: 'Script'

    def __init__(self, script: 'Script', *instances: InertCommand):
        self.instances = WeakSet(instances)
        self.script = script

    def done(self, result: Ok_contra) -> list[InertCommand]:
        """
        Mark all instances as done
        """
        self.val = Done(result)
        return self._advance_all()

    def __repr__(self):
        return f'PrimitiveProxy({repr(set(self.instances))} = {self.val})'

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
        self.verbose = verbose
        self.keep_going = keep_going
        self.store = store

    def collect(self, commands: list[Command[Ok, Err]]) -> Command[list[Ok], Err]:
        """
        Creates a command representing a collection of other commands
        """
        return Gather(commands)

    def callback(self, command: Command[InOk, Err],
            callback: PlainCallback[InOk, Err, Ok]) -> tuple[Command[Ok, Err],
                    list[InertCommand]]:
        """
        Adds an arbitrary callback to an existing command. This triggers
        execution of the command as far as possible, and of the callback if
        ready.
        """

        # Add the command to a strong list and dereference it at the end. This
        # ensures the callback object won't be deleted before getting triggered,
        # but will once it has
        @wraps(callback)
        def _dereference(value: Done[InOk] | Failed[Err]) -> CallbackRet[Ok, Err]:
            ret = callback(value)
            self.pending.remove(cb_command)
            return ret
        cb_command = DeferredCommand(Deferred(_dereference, command))
        self.pending.add(cb_command)

        # The callback is set as a downstream link to the command, but it still
        # need an external trigger, so we need to advance it
        return cb_command, advance_all(self, get_leaves([cb_command]))

    def notify_change(self, command: Command, old_value: Result, new_value: Result):
        """
        Hook called when the graph status changes
        """
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
                if not task_done and isinstance(self, gtt.CoreTaskDef):
                    print_status('PLAN', command.name, task_def.step)

        if isinstance(command, Submit):
            step_name_s = command.task_def.step
            name = command.task_def.name

            # We log 'SUB' every time a submit command's status "changes" to
            # PENDING. Currently this only happens when the command is
            # created.
            match new_value:
                case Pending():
                    print_status('SUB', name, step_name_s)
                case Done():
                    print_status('DONE', name, step_name_s)
                case Failed():
                    print_status('FAIL', name, step_name_s)

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
    primitives : list[InertCommand] = []

    while commands:
        command = commands.pop()
        if not command.is_pending():
            # This should not happen, but can normally be safely ignored
            # when it actually does
            logging.warning('Settled command %s given for updating, skipping', command)
            continue
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
                # For now, subcommands need to be recursively initialized
                sub_commands = command.advance(script)
                if sub_commands:
                    sub_commands = get_leaves(sub_commands)
                    commands.extend(sub_commands)
                # Maybe this caused the command to advance to a terminal state (DONE or
                # FAILED). In that case downstream commands must be checked too.
                elif not command.is_pending():
                    commands.extend(command.outputs)

    return primitives

def get_leaves(commands):
    """
    Collect all the leaves of a command tree

    Leaves can be InertCommands, but also empty Gather([]) commands.
    """
    all_commands = []
    commands = list(commands)
    while commands:
        cmd = commands.pop()
        if not cmd.inputs:
            all_commands.append(cmd)
        else:
            commands.extend(cmd.inputs)
    return all_commands

class Get(NamedPrimitive[gm.Put, str]):
    """
    Get a single resource part
    """

class Submit(InertCommand[gtt.ResultRef, str]):
    """
    Remotely execute a single step
    """
    def __init__(self, task_def: gtt.CoreTaskDef):
        self.task_def = task_def
        super().__init__()

    @property
    def key(self):
        return (self.__class__.__name__.upper(), self.task_def.name)

StatResult: TypeAlias = gm.Found | gm.Done | gm.NotFound

class Stat(NamedPrimitive[StatResult, str]):
    """
    Get a task's metadata
    """

class Put(NamedPrimitive[gtt.ResultRef, str]):
    """
    Upload the object of a literal task
    """
    def __init__(self, name: gtt.TaskName, data: Any):
        super().__init__(name)
        self.data = data

def rget(name: gtt.TaskName) -> Command[list, str]:
    """
    Get a task result, then rescursively get all the sub-parts of it
    """
    return (
        Get(name)
        .then(lambda res: Gather([rget(c.name) for c in res.children]))
        )

def no_not_found(stat_result: StatResult, task: gtt.Task
                 ) -> gm.Found | gm.Done | Failed[str]:
    """
    Transform NotFound in Found if the task is a real object, and fails
    otherwise.
    """
    if not isinstance(stat_result, gm.NotFound):
        return stat_result

    if isinstance(task, gtt.TaskNode):
        return gm.Found(task_def=task.task_def)

    return Failed(f'The task reference {task.name} could not be resolved to a'
        ' definition')

def safe_stat(task: gtt.Task) -> Command[gm.Done | gm.Found, str]:
    """
    Chains no_not_found to a stat
    """
    return (
            Stat(task.name)
            .then(lambda statr: no_not_found(statr, task))
          )

def ssubmit(task: gtt.Task, dry: bool = False
            ) -> Command[gtt.ResultRef, str]:
    """
    A non-recursive ("simple") task submission: executes dependencies, but not
    children. Return said children as result on success.
    """
    return safe_stat(task).then(lambda statr: _ssubmit(task, statr, dry))

def _ssubmit(task: gtt.Task, stat_result: gm.Found | gm.Done, dry: bool
             ) -> gtt.ResultRef | Command[gtt.ResultRef, str]:
    """
    Core ssubmit logic, recurse on dependencies and skip done tasks
    """
    # Short circuit for tasks already processed
    if isinstance(stat_result, gm.Done):
        return stat_result.result

    # gm.Found()
    task_def = stat_result.task_def

    # Short circuit for literals
    if isinstance(task_def, gtt.LiteralTaskDef):
        # Issue #78: at this point we should be making sure children are saved
        # before handing back references to them
        match task:
            case gtt.TaskRef():
                # If we have a reference to a literal, we assume the children
                # definitions have been saved and we can refer to them
                return gtt.ResultRef(task.name,
                        list(map(gtt.TaskRef, task_def.children))
                        )
            case gtt.TaskNode():
                # Else, we can't hand out references but we have the true tasks
                # instead
                return gtt.ResultRef(task.name, task.dependencies)

    # Query, should never have reached this layer as queries have custom run
    # mechanics
    if isinstance(task_def, gtt.QueryTaskDef):
        raise NotImplementedError

    # Finally, Core or Child, process dependencies/parent first

    # Collect dependencies
    deps: list[gtt.Task]
    if isinstance(task, gtt.TaskRef):
        # If a reference, by design the dep defs have been checked in
        deps = [gtt.TaskRef(tin.name)
                for tin in task_def.dependencies(gtt.TaskOp.BASE)]
    else:
        # If a node, we have the defs and pass them directly
        deps = task.dependencies

    # Issue 79: optimize type of command based on type of link
    # Always doing a RSUB will work, but it will run things more eagerly that
    # needed or propagate failures too aggressively.
    gather_deps = Gather([rsubmit(dep, dry) for dep in deps])

    # Issue final command
    if dry or isinstance(task_def, gtt.ChildTaskDef):
        return gather_deps.then(lambda _: gtt.ResultRef(task.name, []))

    return (
            gather_deps
            .then(lambda _: Submit(task_def)) # type: ignore[arg-type] # False positive
            )

def rsubmit(task: gtt.Task, dry: bool = False) -> Command[gtt.RecResultRef, str]:
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
            ssubmit(task, dry)
            .then(lambda res: Gather([rsubmit(c, dry) for c in res.children])
                .then(
                    lambda child_results: gtt.RecResultRef(res, child_results)
                    )
                )
            )

def run(task: gtt.Task, dry=False):
    """
    Combined rsubmit + rget

    If dry, just a dry rsubmit
    """
    if dry:
        return rsubmit(task, dry=True)
    return rsubmit(task).then(lambda _: rget(task.name))

def srun(task: gtt.Task):
    """
    Shallow run: combined ssubmit + get, fetches the raw result of a task but
    not its children
    """
    return ssubmit(task).then(lambda _: Get(task.name))
