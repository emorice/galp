"""
Functional, framework-agnostic, asynchronous programming system
"""

import logging
from weakref import WeakKeyDictionary, WeakSet
from typing import (TypeVar, Generic, Callable, Any, Iterable, TypeAlias,
    Hashable, Mapping, Sequence)
from dataclasses import dataclass, field
from functools import wraps
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

    def eval(self, input_values: list[State]) -> 'tuple[CommandLike[OkT, ErrT], list[Command]]':
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

    def eval(self, input_values: list[State]) -> tuple[CommandLike[OkT, ErrT], list[Command]]:
        """
        State-based handling
        """
        value, = input_values
        # Input wasn't settled, skip
        if isinstance(value, Pending):
            return self, []

        # Input has a Result, we can run callback
        ret = self._state.callback(value)

        # Callback returned a new Command
        if isinstance(ret, Command):
            self._state = Deferred(lambda r: r, ret)
            return ret, [self._state.arg]

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

    def eval(self, input_values: list[State]) -> tuple[
            CommandLike[Sequence[Result[OkT, ErrT]], ErrT],
            list[Command]]:
        """
        State-based handling
        """
        def _as_state(state: State):
            if isinstance(state, Pending):
                return self
            return state
        if self.keep_going:
            return _as_state(none_pending(input_values)), []
        return _as_state(all_ok(input_values)), []

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
    error as soon as the first error is encountered, this will still run all
    other commands (and discard results !) before erroring. A side effect of this
    behavior is that the Error returned is deterministic, since the order
    in which errors are known can not influence the result.
    """
    return _Gather(commands, keep_going).then(result_all_ok)

K = TypeVar('K')

def collect_dict(commands: Mapping[K, CommandLike[OkT, ErrT]], keep_going: bool
                 ) -> Command[dict[K, OkT], ErrT]:
    """
    Wrapper around collect that simplifies gathering dicts of commands
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

@dataclass(eq=False)
class Slot(Generic[OkT, ErrT]):
    """
    Mutable container that holds the current computation state of a future
    """
    state: CommandLike[OkT, ErrT]
    orig_command: Command
    outputs: 'WeakSet[Slot]' = field(default_factory=WeakSet)

class Script:
    """
    Interface to a DAG of promises.
    """
    def __init__(self) -> None:
        # References to all identical primitives
        self.leaves: dict[Hashable, list[Slot]] = {}

        # References to the slots of living commands.
        self.slots: WeakKeyDictionary[Command, Slot]
        self.slots = WeakKeyDictionary()

    def get_value(self, command: Command) -> State:
        """
        Get command value
        """
        if command in self.slots:
            state = self.slots[command].state
            if isinstance(state, Command):
                return Pending()
            return state
        return Pending()

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
        prim_outputs: list[Slot] = []
        for prim in prims:
            prim.state = result
            prim_outputs.extend(prim.outputs)
        return self._advance_all(prim_outputs)

    def init_command(self, command: Command) -> list[InertCommand]:
        """Return the first initial primitives this command depends on"""
        slots, leaves = _make_slots([command])
        self.slots |= slots
        return self._advance_all(leaves)

    def notify_change(self, command: Command, new_value: State) -> None:
        """
        Hook called when the graph status changes
        """

    def _advance_all(self, slots: list[Slot]) -> list[InertCommand]:
        """
        Try to advance all given commands, and all downstream depending on them the
        case being
        """
        primitives : list[InertCommand] = []
        slots = list(slots)

        while slots:
            slot = slots.pop()
            command = slot.orig_command
            cur_state = slot.state
            if not isinstance(cur_state, Command):
                # This should not happen, but can normally be safely ignored
                logging.warning('Settled command %s given for updating, skipping', command)
                continue
            match command:
                case InertCommand():
                    # If it's an output-style primitive, just add it
                    if command.key is None:
                        primitives.append(command)
                        continue
                    # Check if we already have instances of that command
                    if command.key in self.leaves:
                        # If yes, just add it to the list
                        self.leaves[command.key].append(slot)
                    else:
                        # First encounter, initialize the list and send it out
                        self.leaves[command.key] = [slot]
                        primitives.append(command)
                case _:
                    # Else, call its callback to figure out what to do next
                    input_values = [self.get_value(cin) for cin in command.inputs]
                    new_state, new_sub_commands = command.eval(input_values)
                    slot.state = new_state
                    self.notify_change(command,
                        Pending() if isinstance(new_state, Command) else new_state
                        )
                    # eval can have created a new subgraph, so we need to attach it
                    # to the self by filling the output map and collect new leaves
                    if new_sub_commands:
                        new_slots, new_leave_slots = _make_slots(new_sub_commands)
                        # Also add links to current command
                        for cin in command.inputs:
                            new_slots[cin].outputs.add(slot)
                        self.slots |= new_slots
                        slots.extend(new_leave_slots)
                    # Maybe this caused the command to advance to a terminal state
                    # (Result). In that case downstream commands must be checked in turn.
                    elif not isinstance(new_state, Command):
                        slots.extend(slot.outputs)
        return primitives

def _make_slots(commands: Iterable[Command]) -> tuple[dict[Command, Slot], list[Slot]]:
    """
    Browse the dag upstream of the commands given as inputs, initializing
    slots
    """
    # Mapping command -> slot, to ensure we create exactly one slot per
    # command that compares equal. Also acts as a closed set
    all_slots = {}
    # Slots with no children
    leaves = []

    # Open set of commands for which a slot has to be created
    commands = list(commands)
    while commands:
        command = commands.pop()
        # Command was already reached through an other path, skip
        if command in all_slots:
            continue
        # First time seeing this command, create a slot
        slot = Slot(command, command)
        all_slots[command] = slot
        # Add its inputs
        if command.inputs:
            commands.extend(command.inputs)
        else:
            leaves.append(slot)

    # Now we have slots, but no links between them
    for command, slot in all_slots.items():
        for cin in command.inputs:
            all_slots[cin].outputs.add(slot)

    return all_slots, leaves

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
    result = script.get_value(command)
    assert not isinstance(result, Pending)
    return result
