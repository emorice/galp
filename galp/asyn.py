"""
Functional, framework-agnostic, asynchronous programming system
"""

import logging
from weakref import WeakSet, WeakValueDictionary
from typing import (TypeVar, Generic, Callable, Any, Iterable, TypeAlias,
    Hashable)
from dataclasses import dataclass
from itertools import chain
from functools import wraps
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

def state_conj(states: list[State[OkT, ErrT]], keep_going: bool
                ) -> State[list[OkT], ErrT]:
    """
    State conjunction respecting keep_going

    If all commands are Ok, returns Ok with the list of results.
    If any fails, returns Error with one of the errors.

    If keep_going is True, this may return Error when a failure is found
    even if some commands are still Pending (but see Bugs below).

    Bugs: this doesn't look exactly correct, when keep_going is False, it
    looks like this returns Pending in input is (Pending, Error) but Error
    if it's (Failed, Error).
    """
    results = []
    failed = None
    for val in states:
        match val:
            case Pending():
                return Pending()
            case Ok():
                results.append(val.value)
            case Error():
                if not keep_going:
                    return val
                if failed is None:
                    failed = val
    if failed is None:
        return Ok(results)
    return failed

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
        Chain callback to this command on sucess
        """
        return self.eventually(ok_callback(callback))

    def eventually(self, callback: 'PlainCallback[OkT, ErrT, OutOkT]') -> 'Command[OutOkT, ErrT]':
        """
        Chain callback to this command
        """
        return DeferredCommand(Deferred(callback, self))

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
    keep_going: bool

    def __init__(self, commands: 'Command[OkT, ErrT]' | Iterable['Command[OkT, ErrT]'],
                 keep_going: bool):
        super().__init__()
        self.keep_going = keep_going
        self.commands = list(commands) if isinstance(commands, Iterable) else [commands]
        for inp in self.commands:
            inp.outputs.add(self)

    def _eval(self, script: 'Script'):
        """
        State-based handling
        """
        self.val = state_conj([cmd.val for cmd in self.commands], self.keep_going)
        return []

    def __repr__(self):
        return f'Gather({repr(self.commands)} = {repr(self.val)})'

    @property
    def inputs(self):
        """
        Commands we depend on
        """
        return self.commands

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
    def key(self) -> Hashable:
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
        return _advance_all(self.script,
                list(chain.from_iterable(cmd.outputs for cmd in self.instances))
                )

    def __repr__(self):
        return f'PrimitiveProxy({repr(set(self.instances))} = {self.val})'

    def is_pending(self) -> bool:
        """
        Check if pending
        """
        return isinstance(self.val, Pending)

class Script:
    """
    Interface to a DAG of promises.
    """
    def __init__(self) -> None:
        # Strong references to all pending callbacks
        self._pending: set[Command] = set()
        # Weak references to all primitives
        self.commands : WeakValueDictionary[Hashable, _PrimitiveProxy] = WeakValueDictionary()

    def done(self, key: Hashable, result: Result) -> list[InertCommand]:
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

    def collect(self, commands: list[Command[OkT, ErrT]], keep_going: bool
                ) -> Command[list[OkT], ErrT]:
        """
        Creates a command representing a collection of other commands
        """
        return Gather(commands, keep_going)

    def init_command(self, command: Command) -> list[InertCommand]:
        """Return the first initial primitives this command depends on"""
        return _advance_all(self, _get_leaves([command]))

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
        cb_command = command.eventually(_dereference)
        self._pending.add(cb_command)

        # The callback is set as a downstream link to the command, but it still
        # need an external trigger, so we need to advance it
        return cb_command, self.init_command(cb_command)

    def notify_change(self, command: Command, old_value: State,
                      new_value: State) -> None:
        """
        Hook called when the graph status changes
        """

def _advance_all(script: Script, commands: list[Command]) -> list[InertCommand]:
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
                # If it's an output-style primitive, just add it
                if command.key is None:
                    primitives.append(command)
                    continue
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
                    sub_commands = _get_leaves(sub_commands)
                    commands.extend(sub_commands)
                # Maybe this caused the command to advance to a terminal state (DONE or
                # FAILED). In that case downstream commands must be checked too.
                elif not command.is_pending():
                    commands.extend(command.outputs)

    return primitives

def _get_leaves(commands):
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
