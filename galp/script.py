"""
Galp script
"""
from enum import Enum
from collections import defaultdict
from dataclasses import dataclass
from typing import Any

class Trigger(list):
    """
    Trigger interface definition
    """
    def eval(self, states):
        """
        Eval the trigger condition on the given concrete values
        """
        raise NotImplementedError

    def __repr__(self):
        return f'{type(self).__name__}({list.__repr__(self)})'

class AllDone(Trigger):
    """
    Trigger that activates when all deps are DONE, fails when any is FAILED

    This a one-to-one trigger and only consider the first subkey.
    """
    def eval(self, states):
        if all(s[0] == Done.TRUE for s in states):
            return Command.DONE
        if any(s[0] == Done.FALSE for s in states):
            return Command.FAILED
        return Command.UNKNOWN

class AllOver(Trigger):
    """
    Trigger that activates when all deps are either DONE or FAILED.
    Never fails.
    One-to-one trigger.
    """
    def eval(self, states):
        if any(s[0] == Done.UNKNOWN for s in states):
            return Command.UNKNOWN
        return Command.DONE

class NoTrigger(Trigger):
    """
    An empty trigger, that is always considered UNKNOWN and never actually
    triggers.
    """
    def eval(self, states):
        return Command.UNKNOWN

class Done(Enum):
    """
    Ternary state, True/False/Unknown
    """
    UNKNOWN = 'dU'
    FALSE = 'dF'
    TRUE = 'dT'

@dataclass
class Command:
    """
    A command and its known state
    """
    state: defaultdict
    triggered: defaultdict
    downstream: list
    trigger: Any
    callbacks: dict

    # callback conditions
    UNKNOWN = 'U'
    FAILED = 'F'
    DONE = 'D'
    OVER = 'O'


class Script:
    """
    A set of commands with dependencies between them.

    It's essentially a collection of futures, but indexed by an application key,
    and with conveniences to express complicated trigger conditions more easily.
    """
    def __init__(self):
        self._commands = {}

    def done(self, command_key, sub_key=0):
        """
        Marks the corresponding command as successfully done, and runs callbacks
        synchronously
        """
        command = self._commands[command_key]

        # First, we need to write the state of the command since further
        # sister command may need it later
        # We only mark the specific subcommand
        command.state[sub_key] = Done.TRUE

        # Next, we need to check for downstream commands
        # All subcommands have the same
        for down_key in command.downstream:
            self._maybe_trigger(down_key)

    def _maybe_trigger(self, command_key, sub_key=0):
        """
        Check for trigger conditions and call the callback at most once
        """
        command = self._commands[command_key]

        # Reentrancy check
        if command.triggered[sub_key]:
            return

        trigger_state = command.trigger.eval(
            self._commands[dep].state
            for dep in command.trigger
            )

        if trigger_state != Command.UNKNOWN:
            command.triggered[sub_key] = True
            command.state[sub_key] = (
                Done.TRUE
                if trigger_state == Command.DONE
                else Done.FALSE
                )
            callback = command.callbacks[trigger_state]
            if callback:
                callback()

    def add_command(self, command_key, trigger=NoTrigger(), callbacks=None):
        """
        Register a command with callbacks.

        Run the callback immediately if the condition is already satisfied.
        """
        if callbacks is None:
            callbacks = {}
        if (
            (callbacks.get(Command.DONE) or callbacks.get(Command.FAILED))
            and callbacks.get(Command.OVER)
            ):
            raise ValueError('Only one of done/failed or over supported at once')

        if command_key in self._commands:
            raise ValueError(f'Command already added: {command_key}')

        self._commands[command_key] = Command(
            state=defaultdict(lambda: Done.UNKNOWN),
            triggered=defaultdict(bool),
            downstream=[],
            trigger=trigger,
            callbacks={
                state: callbacks.get(state) or callbacks.get(Command.OVER)
                for state in (Command.DONE, Command.FAILED)
                }
            )

        for dep in trigger:
            self._commands[dep].downstream.append(command_key)

        self._maybe_trigger(command_key)

    def __contains__(self, command_key):
        return command_key in self._commands

    def status(self, command_key, sub_key=0):
        """
        Returns status of the command
        """
        return self._commands[command_key].state[sub_key]
