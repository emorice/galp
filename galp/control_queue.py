"""
Queue of items controlled by peer signals
"""
from typing import Iterable, TypeVar, Generic
from collections import deque

T = TypeVar('T')

class ControlQueue(Generic[T]):
    """
    A queue with control flow capabilities
    """
    def __init__(self) -> None:
        self._queue : deque[T] = deque()
        self._can_send: bool = True

    def on_next_item(self) -> T | None:
        """Signals peer can take an item, return one if ready"""
        if self._queue:
            return self._queue.popleft()
        # If we fail a pop on next request, remember we can send the next one
        # right through
        self._can_send = True
        return None

    def push_through(self, items: Iterable[T]) -> list[T]:
        """
        Filter items through the queue, letting only the number allowed by the
        queue state through and queuing the rest.

        Typically, the returned list is a subset of the input limited by buffer
        capacity.
        """
        for item in items:
            self._queue.append(item)
        return self._get_ready_items()

    def _pop(self) -> T | None:
        """Returns the next item to send, if any."""
        # If remote is blocking, don't send anything
        if not self._can_send:
            return None

        if self._queue:
            self._can_send = False
            return self._queue.popleft()
        return None

    def _get_ready_items(self) -> list[T]:
        """
        Return the list of all items that we're allowed to send
        """
        # For now it can only be one
        cmd = self._pop()
        return [] if cmd is None else [cmd]
