"""
Lists of internal commands
"""

import sys
import time
import logging
from weakref import WeakSet, WeakValueDictionary
from typing import (TypeVar, Generic, Callable, Any, Iterable, TypeAlias,
    Sequence, Hashable)
from dataclasses import dataclass
from itertools import chain
from functools import wraps

import galp.net.requests.types as gr
import galp.net.core.types as gm
import galp.task_types as gtt
from galp.serialize import LoadError
from galp.result import Ok, Error, Result

# Extension of Result with a "pending" state
# ==========================================

# pylint: disable=typevar-name-incorrect-variance
OkT = TypeVar('OkT', covariant=True)
ErrT = TypeVar('ErrT', bound=Error)

@dataclass(frozen=True)
class Pending():
    """
    Special type for a result not known yet
    """

State = Pending | Result[OkT, ErrT]

# Commands
# ========

# Helper typevars when a second ok type is needed
InOkT = TypeVar('InOkT')
OutOkT = TypeVar('OutOkT')

@dataclass
class Deferred(Generic[InOkT, OkT, ErrT]):
    """Wraps commands with matching callback"""
    # Gotcha: the order of typevars differs from Callback !
    callback: 'PlainCallback[InOkT, ErrT, OkT]'
    arg: 'Command[InOkT, ErrT]'

    def __repr__(self):
        return f'Deferred({self.callback.__qualname__})'

class Command(Generic[OkT, ErrT]):
    """
    Base class for commands
    """
    def __init__(self) -> None:
        self.val: State[OkT, ErrT] = Pending()
        self.outputs : WeakSet[Command] = WeakSet()

    def then(self, callback: 'Callback[OkT, OutOkT, ErrT]') -> 'Command[OutOkT, ErrT]':
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
        OkT | Result[OkT, ErrT] | Command[OkT, ErrT]
        )
"""
Type for callback returns. We accept callbacks not wrapping a successful result
into an Ok type.
"""
Callback: TypeAlias = Callable[[InOkT], CallbackRet[OkT, ErrT]]
"""
Type of a typical callback
"""
PlainCallback: TypeAlias = Callable[[Result[InOkT, ErrT]], CallbackRet[OkT, ErrT]]
"""
Type of lower-level callback, which should also be called on failures
"""

def ok_callback(callback: Callback[InOkT, OkT, ErrT]
        ) -> PlainCallback[InOkT, ErrT, OkT]:
    """
    Wrap a callback from Ok values to accept and propagate Done/Failed
    accordingly
    """
    @wraps(callback)
    def _ok_callback(val: Result[InOkT, ErrT]):
        return val.then(callback)
    return _ok_callback

class DeferredCommand(Command[OkT, ErrT]):
    """
    A promise of the return value Ok of a callback applied to an input promise
    InOk
    """
    def __init__(self, deferred: Deferred[Any, OkT, ErrT]) -> None:
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
                case Error() | Ok():
                    self.val = ret # type: ignore
                    # Narrowing doesn't quite work here, maybe because the last
                    # case can be about anything and we cannot check the inner
                    # types of Ok/Error
                    return []
                case _:
                    self.val = Ok(ret)
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

class Gather(Command[list[OkT], ErrT]):
    """
    Then-able list
    """
    commands: list[Command[OkT, ErrT]]

    def __init__(self, commands: 'Command[OkT, ErrT]' | Iterable['Command[OkT, ErrT]']):
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

CommandKey: TypeAlias = Hashable

class InertCommand(Command[OkT, ErrT]):
    """
    Command tied to an external event

    Fully defined and identified by a task name
    """
    # Strong ref to master. This allows to make the master dict weak, which in
    # turns drops the master when the last copy gets collected
    # This is purely for memory management and should not be used for any other
    # purpose
    master: '_PrimitiveProxy'

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

Ok_contra = TypeVar('Ok_contra', contravariant=True)

class _PrimitiveProxy(Generic[Ok_contra, ErrT]):
    """
    Helper class to propagate results to several logical instances of the same
    primitive
    """
    instances: WeakSet[InertCommand]
    val: State[Ok_contra, ErrT] = Pending()
    script: 'Script'

    def __init__(self, script: 'Script', *instances: InertCommand):
        self.instances = WeakSet(instances)
        self.script = script

    def done(self, result: Result[Ok_contra, ErrT]) -> list[InertCommand]:
        """
        Mark all instances as done
        """
        self.val = result
        for cmd in self.instances:
            cmd.val = self.val
        return advance_all(self.script,
                list(chain.from_iterable(cmd.outputs for cmd in self.instances))
                )

    def __repr__(self):
        return f'PrimitiveProxy({repr(set(self.instances))} = {self.val})'

    def is_pending(self) -> bool:
        """
        Check if pending
        """
        return isinstance(self.val, Pending)

    @property
    def name(self) -> gtt.TaskName:
        """
        Name of underlying task
        """
        # Primitive proxys can only exist for commands that expose some kind of
        # key, but we haven't defined that properly yet
        return next(ins.name for ins in self.instances) #type: ignore[attr-defined]

class Script:
    """
    Interface to a DAG of promises.

    Args:
        verbose: whether to print brief summary of command completion to stderr
        keep_going: whether to attempt to complete independent commands when one
            fails. If False, attempts to "fail fast" instead, and finishes as
            soon as any command fails.
    """
    def __init__(self, verbose=True, keep_going: bool = False):
        self.verbose = verbose
        self.keep_going = keep_going

        # Strong references to all pending callbacks
        self._pending: set[Command] = set()
        # Weak references to all primitives
        self.commands : WeakValueDictionary[CommandKey, _PrimitiveProxy] = WeakValueDictionary()

    def done(self, key: CommandKey, result: Result) -> list[InertCommand]:
        """
        Mark a command as done, and return new primitives.

        If the command cannot be found or is already done, log a warning and
        ignore the result.
        """
        cmd = self.commands.get(key)
        if cmd is None:
            logging.error('Dropping answer to missing promise %s', key)
            return []
        if not cmd.is_pending():
            logging.error('Dropping answer to finished promise %s', key)
            return []
        return cmd.done(result)

    def collect(self, commands: list[Command[OkT, ErrT]]) -> Command[list[OkT], ErrT]:
        """
        Creates a command representing a collection of other commands
        """
        return Gather(commands)

    def callback(self, command: Command[InOkT, ErrT],
            callback: PlainCallback[InOkT, ErrT, OkT]) -> tuple[Command[OkT, ErrT],
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
        def _dereference(value: Result[InOkT, ErrT]) -> CallbackRet[OkT, ErrT]:
            ret = callback(value)
            self._pending.remove(cb_command)
            return ret
        cb_command = DeferredCommand(Deferred(_dereference, command))
        self._pending.add(cb_command)

        # The callback is set as a downstream link to the command, but it still
        # need an external trigger, so we need to advance it
        return cb_command, advance_all(self, get_leaves([cb_command]))

    def notify_change(self, command: Command, old_value: State,
                      new_value: State) -> None:
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

        if not isinstance(command, Send):
            return
        req = command.request

        if isinstance(req, gm.Stat):
            if isinstance(new_value, Ok):
                task_done, task_def, _children = new_value.value
                if not task_done and isinstance(self, gtt.CoreTaskDef):
                    print_status('PLAN', req.name, task_def.step)

        if isinstance(req, gm.Submit):
            step_name_s = req.task_def.step
            name = req.task_def.name

            # We log 'SUB' every time a submit req's status "changes" to
            # PENDING. Currently this only happens when the req is
            # created.
            match new_value:
                case Pending():
                    print_status('SUB', name, step_name_s)
                case Ok():
                    print_status('DONE', name, step_name_s)
                case Error():
                    print_status('FAIL', name, step_name_s)

    def value_conj(self, commands: list[Command[OkT, ErrT]]
                    ) -> State[list[OkT], ErrT]:
        """
        Value conjunction respecting self.keep_going

        If all commands are Done, returns Done with the list of results.
        If any fails, returns Failed with one of the errors.

        If keep_going is True, this may return Failed when a failure is found
        even if some commands are still Pending (but see Bugs below).

        Bugs: this doesn't look exactly correct, when keep_going is False, it
        looks like this returns Pending in input is (Pending, Failed) but Failed
        if it's (Failed, Pending).
        """
        results = []
        failed = None
        for val in (cmd.val for cmd in commands):
            match val:
                case Pending():
                    return Pending()
                case Ok():
                    results.append(val.value)
                case Error():
                    if not self.keep_going:
                        return val
                    if failed is None:
                        failed = val
        if failed is None:
            return Ok(results)
        return failed

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
                    master = _PrimitiveProxy(script, command)
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

R = TypeVar('R')

def filter_commands(commands: Iterable[InertCommand],
                    filt: Callable[[InertCommand], tuple[Iterable[R], Iterable[InertCommand]]]
                    ) -> list[R]:
    """
    Functional helper to recursively apply to a list of commands a function that
    may fulfill them and generate new commands to filter

    Args:
        commands: list of command to filter
        filt: filter function that takes a command and return two lists of
            objects, the first final objects that should not be filtered any
            more, the other new commands that need recursive filtering.
    """
    commands = list(commands)
    commands.reverse() # So that we .pop() in order
    filtered: list[R] = []
    while commands:
        cmd = commands.pop()
        cur_filtered, cur_tofilter = filt(cmd)
        filtered.extend(cur_filtered)
        commands.extend(cur_tofilter)
    return filtered

# End of machinery and start of app-specific types and logic
# ==========================================================
# To be cut into two modules

class _NamedPrimitive(InertCommand[OkT, ErrT]):
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

class Submit(InertCommand[gtt.ResultRef, Error]):
    """
    Remotely execute a single step
    """
    def __init__(self, task_def: gtt.CoreTaskDef):
        self.task_def = task_def
        super().__init__()

    @property
    def key(self):
        return (self.__class__.__name__.upper(), self.task_def.name)

    @property
    def name(self):
        """Task name"""
        return self.task_def.name

T = TypeVar('T')

class Send(InertCommand[T, Error]):
    """Send an arbitrary request"""
    request: gm.Request
    def __init__(self, request: gm.BaseRequest[T]):
        super().__init__()
        assert isinstance(request, gm.Request) # type: ignore # bug
        self.request = request # type: ignore # guarded by assertion

    @property
    def key(self) -> Hashable:
        return gm.get_request_id(self.request).as_legacy_key()

def safe_deserialize(res: gr.Put, children: Sequence):
    """
    Wrap serializer in a guard for invalid payloads
    """
    match res.deserialize(children):
        case LoadError() as err:
            return err
        case Ok(result):
            return result

def rget(name: gtt.TaskName) -> Command[Any, Error]:
    """
    Get a task result, then rescursively get all the sub-parts of it
    """
    return (
        Send(gm.Get(name))
        .then(lambda res: (
            Gather([rget(c.name) for c in res.children])
            .then(lambda children: safe_deserialize(res, children))
            ))
        )

def sget(name: gtt.TaskName) -> Command[Any, Error]:
    """
    Shallow or simple get: get a task result, and deserialize it but keeping
    children as references instead of recursing on them like rget
    """
    return (
        Send(gm.Get(name))
        .then(lambda res: safe_deserialize(res, res.children))
        )

def no_not_found(stat_result: gr.StatReplyValue, task: gtt.Task
                 ) -> gr.Found | gr.StatDone | Error:
    """
    Transform NotFound in Found if the task is a real object, and fails
    otherwise.
    """
    if not isinstance(stat_result, gr.NotFound):
        return stat_result

    if isinstance(task, gtt.TaskNode):
        return gr.Found(task_def=task.task_def)

    return Error(f'The task reference {task.name} could not be resolved to a'
        ' definition')

def safe_stat(task: gtt.Task) -> Command[gr.StatDone | gr.Found, Error]:
    """
    Chains no_not_found to a stat
    """
    return (
            Send(gm.Stat(task.name))
            .then(lambda statr: no_not_found(statr, task))
          )

def ssubmit(task: gtt.Task, dry: bool = False
            ) -> Command[gtt.ResultRef, Error]:
    """
    A non-recursive ("simple") task submission: executes dependencies, but not
    children. Return said children as result on success.
    """
    return safe_stat(task).then(lambda statr: _ssubmit(task, statr, dry))

def _ssubmit(task: gtt.Task, stat_result: gr.Found | gr.StatDone, dry: bool
             ) -> gtt.ResultRef | Command[gtt.ResultRef, Error]:
    """
    Core ssubmit logic, recurse on dependencies and skip done tasks
    """
    # Short circuit for tasks already processed
    if isinstance(stat_result, gr.StatDone):
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

def rsubmit(task: gtt.Task, dry: bool = False) -> Command[gtt.RecResultRef, Error]:
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

def run(task: gtt.Task, dry=False) -> Command[Any, Error]:
    """
    Combined rsubmit + rget

    If dry, just a dry rsubmit
    """
    if dry:
        return rsubmit(task, dry=True)
    return rsubmit(task).then(lambda _: rget(task.name))

def srun(task: gtt.Task):
    """
    Shallow run: combined ssubmit + sget, fetches the raw result of a task but
    not its children
    """
    return ssubmit(task).then(lambda _: sget(task.name))
