"""
Galp script
"""
from dataclasses import dataclass
from typing import Any

class AllDone(list):
    """
    Trigger that activates when all deps are DONE, fails when any is FAILED
    """
    def eval(self, states):
        """
        Eval the trigger
        """
        if all(s == Command.DONE for s in states):
            return Command.DONE
        if any(s == Command.FAILED for s in states):
            return Command.FAILED
        return Command.UNKNOWN

class AllOver(list):
    """
    Trigger that activates when all deps are either DONE or FAILED.
    Cannot fail.
    """

class NoTrigger(list):
    """
    Empty trigger
    """

@dataclass
class Command:
    """
    A command and its known state
    """
    status: int
    triggered: bool
    downstream: list
    trigger: Any
    callbacks: dict

    # states
    UNKNOWN = 0
    FAILED = 1
    DONE = 2
    OVER = 3


class Script:
    """
    A set of commands with dependencies between them.

    It's essentially a collection of futures, but indexed by an application key,
    and with conveniences to express complicated trigger conditions more easily.
    """
    def __init__(self):
        self._commands = {}

    def done(self, command_key):
        """
        Marks the corresponding command as successfully done, and runs callbacks
        synchronously
        """
        command = self._commands[command_key]

        # First, we need to write the status of the command since further
        # sister command may need it later
        command.status = Command.DONE

        # Next, we need to check for downstream commands
        for down_key in command.downstream:
            self._maybe_trigger(down_key)

    def _maybe_trigger(self, command_key):
        """
        Check for trigger conditions and call the callback at most once
        """
        command = self._commands[command_key]

        # Reentrancy check
        if command.triggered:
            return

        trigger_state = command.trigger.eval(
            self._commands[dep].status
            for dep in command.trigger
            )

        if trigger_state != Command.UNKNOWN:
            command.triggered = True
            callback = command.callbacks[trigger_state]
            if callback:
                callback()

    def add_command(self, command_key, trigger=NoTrigger(), callbacks=None):
        """
        Register a command with callbacks.
        """
        if callbacks is None:
            callbacks = {}
        if (
            (callbacks.get(Command.DONE) or callbacks.get(Command.FAILED))
            and callbacks.get(Command.OVER)
            ):
            raise ValueError('Only one of done/failed or over supported at once')

        if command_key in self._commands:
            raise ValueError('Command already added')

        self._commands[command_key] = Command(
            status=Command.UNKNOWN,
            triggered=False,
            downstream=[],
            trigger=trigger,
            callbacks={
                state: callbacks.get(state) or callbacks.get(Command.OVER)
                for state in (Command.DONE, Command.FAILED)
                }
            )

        for dep in trigger:
            self._commands[dep].downstream.append(command_key)
