"""
Queue of active commands
"""
from typing import Iterable
from collections import deque

import galp.commands as cm

class CommandQueue:
    """
    A queue of active commands that need periodic re-issuing
    """
    def __init__(self) -> None:
        self._queue : deque[cm.InertCommand] = deque()
        self._can_send: bool = True

    def on_next_request(self) -> cm.InertCommand | None:
        """Returns next command to send, no matter the time left"""
        if self._queue:
            return self._queue.popleft()
        # If we fail a pop on next request, remember we can send the next one
        # right through
        self._can_send = True
        return None

    def enqueue(self, commands: Iterable[cm.InertCommand]) -> list[cm.InertCommand]:
        """
        Adds a command to the end of queue, and return any command ready to be
        sent.

        Typically, the returned list is a subset of the input limited by buffer
        capacity.
        """
        for command in commands:
            self._queue.append(command)
        return self._get_ready_commands()

    def _pop(self) -> cm.InertCommand | None:
        """
        Returns the next command to send, if any.
        """
        # If remote is blocking, don't send anything
        if not self._can_send:
            return None

        if self._queue:
            self._can_send = False
            return self._queue.popleft()
        return None

    def _get_ready_commands(self) -> list[cm.InertCommand]:
        """
        Return the list of all commands that we're allowed to send
        """
        # For now it can only be one
        cmd = self._pop()
        return [] if cmd is None else [cmd]
