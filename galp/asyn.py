"""
Functional, framework-agnostic, asynchronous programming system
"""

import logging
from weakref import WeakKeyDictionary, WeakSet
from typing import (TypeVar, Generic, Callable, Iterable, TypeAlias,
    Hashable, Mapping, Sequence)
from dataclasses import dataclass, field
from functools import wraps
from galp.result import Ok, Error, Result, all_ok as result_all_ok

# Extension of Result with a "pending" state
# ==========================================

# pylint: disable=typevar-name-incorrect-variance
OkT = TypeVar('OkT', covariant=True)
ErrT = TypeVar('ErrT', bound=Error)

State = Result[OkT, ErrT] | None

def none_pending(states: Iterable[State[OkT, ErrT]]
                ) -> Ok[list[Result[OkT, ErrT]]] | None:
    """
    Return None if any state is None, else Ok with the list of results
    """
    results = []
    for state in states:
        if state is None:
            return None
        results.append(state)
    return Ok(results)

def all_ok(states: Iterable[State[OkT, ErrT]]
           ) -> State[list[Ok[OkT]], ErrT]:
    """
    Return Error if any state is Error, else None if any state is None,
    else Ok with the list of values.
    """
    results = []
    any_pending = False

    for state in states:
        match state:
            case None:
                any_pending = True
                # We still need to continue to check Errors
            case Ok():
                results.append(state)
            case Error():
                return state
    if any_pending:
        return None

    return Ok(results)

# Commands
# ========

# Helper typevars when a second ok type is needed
InOkT = TypeVar('InOkT')
OutOkT = TypeVar('OutOkT')

class Command(Generic[OkT, ErrT]):
    """
    Base class for commands
    """
    def __init__(self, inputs: 'list[Command]'):
        self.inputs = inputs

    def then(self, callback: 'Callback[OkT, OutOkT, ErrT]') -> 'Command[OutOkT, ErrT]':
        """
        Chain callback to this command on sucess
        """
        return self.eventually(ok_callback(callback))

    def eventually(self, callback: 'PlainCallback[OkT, ErrT, OutOkT]') -> 'Command[OutOkT, ErrT]':
        """
        Chain callback to this command
        """
        return DeferredCommand(self, callback)

    def eval(self, input_values: list[State]) -> 'CommandLike[OkT, ErrT] | None':
        """
        Logic of calculating the value
        """
        raise NotImplementedError

class ResultCommand(Command[OkT, ErrT]):
    """
    Command wrapping a Result

    Needlessly convoluted because current code assumes all commands are started
    in Pending state, and are updated at least once before settling.
    """
    def __init__(self, result: Result[OkT, ErrT]):
        super().__init__([])
        self._result = result

    def eval(self, _input_values: list[State]) -> 'CommandLike[OkT, ErrT] | None':
        assert not _input_values
        return self._result

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
    def __init__(self, command: Command[InOkT, ErrT],
                 callback: PlainCallback[InOkT, ErrT, OkT]) -> None:
        super().__init__([command])
        self.callback = callback

    def eval(self, input_values: list[State]) -> 'CommandLike[OkT, ErrT] | None':
        """
        State-based handling
        """
        value, = input_values
        # Input wasn't settled, skip
        if value is None:
            return None

        # Input has a Result, we can run callback
        ret = self.callback(value)

        # Callback returned a new Command
        if isinstance(ret, Command):
            return ResolvedCommand(ret)

        # Callback returned a concrete value
        return ret

class ResolvedCommand(Command[OkT, ErrT]):
    """Command shadowing an other"""
    def __init__(self, command: Command[OkT, ErrT]):
        super().__init__([command])
        self.command = command

    def eval(self, input_values: list[State]) -> 'CommandLike[OkT, ErrT] | None':
        value, = input_values
        return value

class _Gather(Command[Sequence[Result[OkT, ErrT]], ErrT]):
    """
    Then-able list with state conjunction respecting keep_going

    If keep_going is False, this fails as soon as any input fails. Otherwise, it
    returns None until all the inputs are Ok, at which point it returns a
    list of Ok.

    If keep_going is False, this stays None until all states are Ok or
    Failed, at which point it returns a list of them.
    """
    commands: list[Command[OkT, ErrT]]
    keep_going: bool

    def __init__(self, commands: CommandLike[OkT, ErrT] |
                 Iterable[CommandLike[OkT, ErrT]],
                 keep_going: bool):
        _list = list(commands) if isinstance(commands, Iterable) else [commands]
        super().__init__([as_command(cmdlike) for cmdlike in _list])
        self.keep_going = keep_going

    def eval(self, input_values: list[State]
             ) -> CommandLike[Sequence[Result[OkT, ErrT]], ErrT] | None:
        """
        State-based handling
        """
        if self.keep_going:
            return none_pending(input_values)
        return all_ok(input_values)

    def __repr__(self):
        return f'Gather({repr(self.inputs)})'

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

class Primitive(Command[OkT, ErrT]):
    """
    Command tied to an external event

    Fully defined and identified by a task name
    """
    def __init__(self):
        super().__init__([])

    def eval(self, *_) -> None:
        return None

    @property
    def key(self) -> Hashable:
        """
        Unique key
        """
        raise NotImplementedError


@dataclass
class Pending(Generic[OkT, ErrT]):
    """
    Pending slot state pointing to other slots
    """
    inputs: 'list[Slot]'
    update: Callable[[list[State]], CommandLike[OkT, ErrT] | None]

@dataclass(eq=False)
class Slot(Generic[OkT, ErrT]):
    """
    Mutable container that holds the current computation state of a future
    """
    state: Result[OkT, ErrT] | Primitive[OkT, ErrT] | Pending[OkT, ErrT]
    outputs: 'WeakSet[Slot]' = field(default_factory=WeakSet)

    def get_value(self) -> State[OkT, ErrT]:
        """
        Value
        """
        if isinstance(self.state, Command | Pending):
            return None
        return self.state

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
            return self.slots[command].get_value()
        return None

    def done(self, key: Hashable, result: Result) -> list[Primitive]:
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

    def init_command(self, command: Command) -> list[Primitive]:
        """Return the first initial primitives this command depends on"""
        slots, leaves = _make_slots([command])
        self.slots |= slots
        return self._advance_all(leaves)

    def _advance_all(self, slots: list[Slot]) -> list[Primitive]:
        """
        Try to advance all given commands, and all downstream depending on them the
        case being
        """
        primitives : list[Primitive] = []
        slots = list(slots)

        while slots:
            slot = slots.pop()
            cur_state = slot.state
            match cur_state:
                case Primitive():
                    # If it's an output-style primitive, just add it
                    if cur_state.key is None:
                        primitives.append(cur_state)
                        continue
                    # Check if we already have instances of that command
                    if cur_state.key in self.leaves:
                        # If yes, just add it to the list
                        self.leaves[cur_state.key].append(slot)
                    else:
                        # First encounter, initialize the list and send it out
                        self.leaves[cur_state.key] = [slot]
                        primitives.append(cur_state)
                case Pending():
                    # Else, call its callback to figure out what to do next
                    input_values = [sin.get_value() for sin in cur_state.inputs]
                    new_state = cur_state.update(input_values)
                    match new_state:
                        case None:
                            # Command skipped, skip too
                            pass
                        case Command():
                            # Command resolved to a non-trivial new command
                            new_slots, new_leaf_slots = _make_slots(new_state.inputs)
                            # Also add links to current command
                            new_inputs = []
                            for cin in new_state.inputs:
                                slot_in = new_slots[cin]
                                slot_in.outputs.add(slot)
                                new_inputs.append(slot_in)
                            self.slots |= new_slots
                            slots.extend(new_leaf_slots)
                            slot.state = Pending(new_inputs, new_state.eval)
                        case _:
                            # Command settled (Result). In that case downstream
                            # commands must be checked in turn.
                            slot.state = new_state
                            slots.extend(slot.outputs)
                case _: # Result
                    # This should not happen, but can normally be safely ignored
                    logging.warning('Settled command %s given for updating, skipping', slot)
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
        if isinstance(command, Primitive):
            slot = Slot(command)
        else:
            slot = Slot(Pending([], command.eval))
        all_slots[command] = slot
        # Add its inputs
        if command.inputs:
            commands.extend(command.inputs)
        else:
            leaves.append(slot)

    # Now we have slots, but no links between them
    for command, slot in all_slots.items():
        if not isinstance(slot.state, Pending):
            continue
        for cin in command.inputs:
            all_slots[cin].outputs.add(slot)
            slot.state.inputs.append(all_slots[cin])

    return all_slots, leaves

R = TypeVar('R')

def filter_commands(commands: Iterable[Primitive],
                    filt: Callable[[Primitive], tuple[Iterable[R], Iterable[Primitive]]]
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
                callback: Callable[[Primitive], Result]
                ) -> Result[OkT, ErrT]:
    """
    Run command by fulfilling primitives with given synchronous callback
    """
    script = Script()
    primitives = script.init_command(command)
    def _filter(prim: Primitive) -> tuple[list[None], list[Primitive]]:
        return [], script.done(prim.key, Ok(callback(prim)))
    unprocessed = filter_commands(primitives, _filter)
    assert not unprocessed
    result = script.get_value(command)
    assert result is not None
    return result
