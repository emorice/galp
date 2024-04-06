"""
Functional, framework-agnostic, asynchronous programming system
"""

import logging
from weakref import WeakKeyDictionary
from typing import (TypeVar, Generic, Callable, Any, Iterable, TypeAlias,
    Hashable, Mapping, Sequence)
from dataclasses import dataclass
from functools import wraps
from collections import defaultdict
from galp.result import Ok, Error, Result, all_ok as result_all_ok

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

def none_pending(states: Iterable[State[OkT, ErrT]]
                ) -> Ok[list[Result[OkT, ErrT]]] | Pending:
    """
    Return Pending if any state is pending, else Ok with the list of results
    """
    results = []
    for state in states:
        if isinstance(state, Pending):
            return Pending()
        results.append(state)
    return Ok(results)

def all_ok(states: Iterable[State[OkT, ErrT]]
           ) -> State[list[Ok[OkT]], ErrT]:
    """
    Return Error if any state is Error, else Pending if any state is Pending,
    else Ok with the list of values.
    """
    results = []
    any_pending = False

    for state in states:
        match state:
            case Pending():
                any_pending = True
                # We still need to continue to check Errors
            case Ok():
                results.append(state)
            case Error():
                return state
    if any_pending:
        return Pending()

    return Ok(results)

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

    def eval(self, input_values: list[State]) -> 'tuple[State, list[Command]]':
        """
        Logic of calculating the value
        """
        raise NotImplementedError

    @property
    def inputs(self) -> 'list[Command]':
        """
        Other commands we depend on
        """
        raise NotImplementedError

class ResultCommand(Command[OkT, ErrT]):
    """
    Command wrapping a Result

    Needlessly convoluted because current code assumes all commands are started
    in Pending state, and are updated at least once before settling.
    """
    def __init__(self, result: Result[OkT, ErrT]):
        self._result = result
        super().__init__()

    def eval(self, _input_values):
        assert not _input_values
        return self._result, []

    @property
    def inputs(self):
        return []

CommandLike: TypeAlias = Result[OkT, ErrT] | Command[OkT, ErrT]

def as_command(obj: CommandLike[OkT, ErrT]) -> Command[OkT, ErrT]:
    """
    Wrap a result into a (done) command.

    Used by code that needs to manipulate mixed collections of commands and
    results
    """
    if isinstance(obj, Command):
        return obj
    return ResultCommand(obj)

Callback: TypeAlias = Callable[[InOkT], CommandLike[OkT, ErrT]]
"""
Type of a typical callback
"""
PlainCallback: TypeAlias = Callable[[Result[InOkT, ErrT]], CommandLike[OkT, ErrT]]
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

    def __repr__(self):
        return f'DeferredCommand(state={repr(self._state)})'

    def eval(self, input_values: list[State]) -> tuple[State, list[Command]]:
        """
        State-based handling
        """
        value, = input_values
        # Input wasn't settled, skip
        if isinstance(value, Pending):
            return Pending(), []

        # Input has a Result, we can run callback
        ret = self._state.callback(value)

        # Callback returned a new Command
        if isinstance(ret, Command):
            self._state = Deferred(lambda r: r, ret)
            return Pending(), [self._state.arg]

        # Callback returned a concrete value
        return ret, []

    @property
    def inputs(self) -> list[Command]:
        """
        Commands we depend on
        """
        return [self._state.arg]

class _Gather(Command[Sequence[Result[OkT, ErrT]], ErrT]):
    """
    Then-able list with state conjunction respecting keep_going

    If keep_going is False, this fails as soon as any input fails. Otherwise, it
    returns Pending until all the inputs are Ok, at which point it returns a
    list of Ok.

    If keep_going is False, this stays Pending until all states are Ok or
    Failed, at which point it returns a list of them.
    """
    commands: list[Command[OkT, ErrT]]
    keep_going: bool

    def __init__(self, commands: CommandLike[OkT, ErrT] |
                 Iterable[CommandLike[OkT, ErrT]],
                 keep_going: bool):
        super().__init__()
        self.keep_going = keep_going
        _list = list(commands) if isinstance(commands, Iterable) else [commands]
        self.commands = [as_command(cmdlike) for cmdlike in _list]

    def eval(self, input_values: list[State]):
        """
        State-based handling
        """
        if self.keep_going:
            return none_pending(input_values), []
        return all_ok(input_values), []

    def __repr__(self):
        return f'Gather({repr(self.commands)})'

    @property
    def inputs(self) -> list[Command]:
        """
        Commands we depend on
        """
        return self.commands

def collect_all(commands: Iterable[CommandLike[OkT, ErrT]], keep_going: bool
            ) -> Command[Sequence[Result[OkT, ErrT]], ErrT]:
    """
    Gather all commands, continuing on Error according to keep_going.

    When finished, returns a list of all results. If keep_going is false, this
    list will contain only Ok as the whole command will error early. If
    keep_going is true, the final list will contain possibly both Ok and Error,
    and the whole command will never itself Error.

    Because these error handling subtleties are quickly tricky, only use this
    when you really need the flexibility. See collect for a more straightforward
    interface.
    """
    return _Gather(commands, keep_going)

def collect(commands: Iterable[CommandLike[OkT, ErrT]], keep_going: bool
            ) -> Command[list[OkT], ErrT]:
    """
    Gather all commands, continuing on Error according to keep_going.

    When finished, returns Error if at least one was encountered, else the list
    of successful values.

    Note that with keep_going set to True, even if the result can be known to be an
    error as soon as the first error is encoutered, this will still run all
    other commands (and discard results !) before erroring. A side effect of this
    behavior is that the Error returned is deterministic, since the order
    in which errors are known can not influence the result.
    """
    return _Gather(commands, keep_going).then(result_all_ok)

K = TypeVar('K')

def collect_dict(commands: Mapping[K, CommandLike[OkT, ErrT]], keep_going: bool
                 ) -> Command[dict[K, OkT], ErrT]:
    """
    Wrapper around collect that simplies gathering dicts of commands
    """
    keys, values = list(commands.keys()), list(commands.values())
    return collect(values, keep_going).then(
            lambda ok_values: Ok(dict(zip(keys, ok_values)))
            )

class InertCommand(Command[OkT, ErrT]):
    """
    Command tied to an external event

    Fully defined and identified by a task name
    """

    def eval(self, *_):
        # Never changes by itself
        return Pending(), []

    @property
    def key(self) -> Hashable:
        """
        Unique key
        """
        raise NotImplementedError

    @property
    def inputs(self) -> list[Command]:
        """
        Commands we depend on
        """
        return []

Ok_contra = TypeVar('Ok_contra', contravariant=True)

class Script:
    """
    Interface to a DAG of promises.
    """
    def __init__(self) -> None:
        # References to all identical primitives
        self.leaves: dict[Hashable, list[InertCommand]] = {}

        # Inverse map of inputs
        self.outputs: WeakKeyDictionary[Command, list[Command]]
        self.outputs = WeakKeyDictionary()

        self.values: WeakKeyDictionary[Command, State]
        self.values = WeakKeyDictionary()

    def done(self, key: Hashable, result: Result) -> list[InertCommand]:
        """
        Mark a command as done, and return new primitives.

        If the command cannot be found or is already done, log a warning and
        ignore the result.
        """
        if key not in self.leaves:
            logging.error('Received unexpected result to command %s', key)
            return []
        # Propagate to all current instances and clear them
        prims = self.leaves.pop(key)
        prim_outputs = []
        for prim in prims:
            self.values[prim] = result
            prim_outputs.extend(
                    self.outputs.get(prim) or []
                    )
        return _advance_all(self, prim_outputs)

    def init_command(self, command: Command) -> list[InertCommand]:
        """Return the first initial primitives this command depends on"""
        first_leaves, outs = _get_leaves([command])
        self.outputs |= outs
        return _advance_all(self, first_leaves)

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
        cur_value = script.values.get(command, Pending())
        if not isinstance(cur_value, Pending):
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
                if command.key in script.leaves:
                    # If yes, just add it to the list
                    script.leaves[command.key].append(command)
                else:
                    # First encounter, initialize the list and send it out
                    script.leaves[command.key] = [command]
                    primitives.append(command)
            case _:
                # Else, call its callback to figure out what to do next
                input_values = [script.values.get(cin, Pending())
                                for cin in command.inputs]
                new_value, new_sub_commands = command.eval(input_values)
                script.values[command] = new_value
                script.notify_change(command, cur_value, new_value)
                # eval can have created a new subgraph, so we need to attach it
                # to the script by filling the output map and collect new leaves
                if new_sub_commands:
                    new_leaves, new_outs = _get_leaves(new_sub_commands)
                    # Also add links to current command
                    for cin in command.inputs:
                        new_outs[cin].append(command)
                    script.outputs |= new_outs
                    commands.extend(new_leaves)
                # Maybe this caused the command to advance to a terminal state
                # (Result). In that case downstream commands must be checked in turn.
                elif not isinstance(new_value, Pending):
                    commands.extend(script.outputs.get(command) or [])

    return primitives

def _get_leaves(commands: Iterable[Command]
                ) -> tuple[list[Command], Mapping[Command, list[Command]]]:
    """
    Collect all the leaves and outputs of a command tree

    Leaves can be InertCommands, empty Gather([]) commands, or constants.
    """
    leaves = []
    outputs = defaultdict(list)
    commands = list(commands)
    while commands:
        cmd = commands.pop()
        for cin in cmd.inputs:
            outputs[cin].append(cmd)
        if not cmd.inputs:
            leaves.append(cmd)
        else:
            commands.extend(cmd.inputs)
    return leaves, outputs

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

def run_command(command: Command[OkT, ErrT],
                callback: Callable[[InertCommand], Result]
                ) -> Result[OkT, ErrT]:
    """
    Run command by fulfilling primitives with given synchronous callback
    """
    script = Script()
    primitives = script.init_command(command)
    def _filter(prim: InertCommand) -> tuple[list[None], list[InertCommand]]:
        return [], script.done(prim.key, Ok(callback(prim)))
    unprocessed = filter_commands(primitives, _filter)
    assert not unprocessed
    result = script.values[command]
    assert not isinstance(result, Pending)
    return result
