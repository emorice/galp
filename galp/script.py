"""
Galp script
"""
from collections import defaultdict, namedtuple
from dataclasses import dataclass
from typing import Any
import logging

class Trigger(list):
    """
    Trigger interface definition
    """
    def eval(self, command):
        """
        Eval the trigger condition on the given concrete values
        """
        if not command.inputs:
            command.inputs = [(
                    command.definition.script
                    .get_def(command_key)
                    .pull_command(
                        (trigger_param,) + command.path,
                        command)
                ) for command_key, trigger_param in self
            ]
        defs = [command.definition.script._commands[dep] for dep in self]
        return self.eval_defs(command, defs)


    def eval_defs(self, command, defs):
        """
        Eval the trigger condition on the given concrete values
        """
        raise NotImplementedError

    def downlinks(self):
        """
        Returns a list of deps and path mapping functions
        """
        return [
            (dep, lambda path: path)
            for dep in self
            ]

    def __repr__(self):
        return f'{type(self).__name__}({list.__repr__(self)})'

def status_conj(status):
    """
    Ternary conjunction of an iterable of status
    """
    if all(s == Command.DONE for s in status):
        return Command.DONE
    if any(s == Command.FAILED for s in status):
        return Command.FAILED
    return Command.UNKNOWN

class AllDone(Trigger):
    """
    Trigger that activates when all deps are DONE, fails when any is FAILED

    This a one-to-one trigger and only consider the first subkey.
    """
    def eval_defs(self, command, defs):
        input_status = [cdef.status[command.path] for cdef in defs]
        status = status_conj(input_status)

        logging.info('ALLDONE path %s: %s -> %s', command.path,
            input_status, status
            )
        return status


class AllOver(Trigger):
    """
    Trigger that activates when all deps are either DONE or FAILED.
    Never fails.
    One-to-one trigger.
    """
    def eval_defs(self, command, deps):
        if any(dep.status[command.path] == Command.UNKNOWN for dep in deps):
            return Command.UNKNOWN
        return Command.DONE

class ForLoop:
    """
    Trigger that activates when a number of subcommands of an other have
    completed.
    """
    def __init__(self, length, body):
        self.length = length
        self.body = body

    def eval(self, command):
        """
        Eval the trigger condition on the given concrete values
        """
        if not command.inputs:
            command.inputs = {}
        if not 'length' in command.inputs:
            command.inputs['length'] = (
                    command.definition.script
                    .get_def(self.length)
                    .pull_command(command.param, command)
                    )
        return Command.UNKNOWN

    def downlinks(self):
        return []

class SubsDone(Trigger):
    """
    Trigger that activates when a number of subcommands of an other have
    completed.
    """
    def eval_defs(self, command, deps):
        counter, dep = deps
        # If the counter is failed or unk, so is this
        if counter.status[command.path] != Command.DONE:
            return counter.status[command.path]
        n_subs = counter.result[command.path]
        input_status = [
            dep.status[command.path + (i,)]
            for i in range(n_subs)
            ]
        status =  status_conj(input_status)
        logging.info('SUBSDONE path %s: %s -> %s', command.path,
            input_status, status
            )
        return status

    def downlinks(self):
        counter, dep = self
        return [
            (counter, lambda path: path),
            (dep, lambda path: path[:-1] if path else None)
            ]

class NoTrigger(Trigger):
    """
    An empty trigger, that is always considered UNKNOWN and never actually
    triggers.
    """
    def eval_defs(self, command, defs):
        return Command.UNKNOWN

@dataclass
class CommandDefinition:
    """
    A command and its known status
    """
    status: defaultdict
    triggered: defaultdict
    downstream: list
    trigger: Any
    callback: Any
    result: dict
    key: Any
    script: Any

    # callback conditions
    UNKNOWN = 'U'
    FAILED = 'F'
    DONE = 'D'
    OVER = 'O'

    def pull_command(self, param, parent):
        """
        Instantiates a new command tracking structure, corresponding to one
        specific invocation of the command.

        When the command instance is created, its trigger is checked, possibly
        leading to the creation of other commands recursively, and possibly
        leading to the callbacks being called if the trigger consition is
        satisfied.
        """
        # obsolete parameter
        path = tuple(param[1:]) if param else tuple()
        command = _Command(
            definition=self,
            path=path,
            inputs=None,
            outputs=[parent]
            )
        logging.info('Creating command %s / %s',
            (self.key, param[0] if param else None),
            path
            )
        self.script._maybe_trigger_cmd(command)
        return command

Command = CommandDefinition

@dataclass
class _Command:
    """
    A command and its known status

    Prototype that represent only one path instance
    """
    definition: CommandDefinition
    path: tuple
    inputs: Any
    outputs: Any = tuple()
    param: Any = None

class Script:
    """
    A set of commands with dependencies between them.

    It's essentially a collection of futures, but indexed by an application key,
    and with conveniences to express complicated trigger conditions more easily.
    """
    def __init__(self):
        self._commands = {}

    def done(self, command_key, path=tuple(), result=None):
        """
        Marks the corresponding command as successfully done, and runs callbacks
        synchronously.

        If no path is given, the root instance of the command is used.
        """
        return self.over(command_key, Command.DONE, path, result)

    def over(self, command_key, status, path=tuple(), result=None):
        """
        Mark a command as terminated with the given status, and triggers
        callbacks if necessery.
        """
        command_def = self._commands[command_key]

        # First, we need to write the status of the command_def since further
        # sister command_def may need it later
        # We only mark the specific subcommand
        command_def.status[path] = status
        command_def.result[path] = result

        # Next, we need to check for downstream commands
        # All subcommands have the same
        for down_key, pathmap in command_def.downstream:
            down_path = pathmap(path)
            logging.error('DOWN %s %s', down_key, down_path)
            if down_path is not None:
                self._maybe_trigger(down_key, down_path)

    def _maybe_trigger_cmd(self, command):
        """
        Check for trigger conditions and call the callback at most once
        """
        command_key = command.definition.key
        path = command.path

        command_def = self._commands[command_key]

        # Reentrancy check
        if command_def.triggered[path]:
            return

        trigger_status = command_def.trigger.eval(command)

        if trigger_status != Command.UNKNOWN:
            command_def.triggered[path] = True
            command_def.status[path] = (
                Command.DONE
                if trigger_status == Command.DONE
                else Command.FAILED
                )
            if command_def.callback:
                command_def.callback(path, trigger_status)

    def _maybe_trigger(self, command_key, path):
        logging.warning('Obsolete _maybe_trigger called, use _maybe_trigger_cmd')
        command_def = self._commands[command_key]

        # Reentrancy check
        if command_def.triggered[path]:
            return

        trigger_status = command_def.trigger.eval(
            # Wrong, command has a state
            _Command(command_def, path, None) 
            )

        if trigger_status != Command.UNKNOWN:
            command_def.triggered[path] = True
            command_def.status[path] = (
                Command.DONE
                if trigger_status == Command.DONE
                else Command.FAILED
                )
            if command_def.callback:
                command_def.callback(path, trigger_status)

    def add_command(self, command_key, trigger=NoTrigger(), callback=None):
        logging.warning('Obsolete add_command called, use define_command')
        return self.define_command(command_key, trigger, callback)

    def define_command(self, command_key, trigger=NoTrigger(), callback=None):
        """
        Register a command with callbacks.

        Run the callback immediately if the condition is already satisfied.
        """
        if command_key in self._commands:
            raise ValueError(f'Command already added: {command_key}')

        command_def = CommandDefinition(
            status=defaultdict(lambda: Command.UNKNOWN),
            triggered=defaultdict(bool),
            downstream=[],
            trigger=trigger,
            callback=callback,
            result={},
            key=command_key,
            script=self,
            )
        self._commands[command_key] = command_def

        for dep, pathmap in trigger.downlinks():
            self._commands[dep].downstream.append((command_key, pathmap))

        #self._maybe_trigger(command_key, path=tuple())

        return command_def

    def run(self, command_def):
        """
        Creates a concrete command from a command definition, potentially
        triggering its execution
        """
        command = _Command(command_def, path=tuple(), inputs=None)

        self._maybe_trigger_cmd(command)

        return command

    def __contains__(self, command_key):
        return command_key in self._commands

    def status(self, command_key, path=tuple()):
        """
        Returns status of the command
        """
        return self._commands[command_key].status[path]

    def get_def(self, command_key):
        return self._commands[command_key]
