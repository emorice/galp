"""
Queue of active commands
"""

import time
from collections import deque

import galp.commands as cm

class CommandQueue:
    """
    A queue of active commands that need periodic re-issuing
    """
    def __init__(self, retry_interval: float=0.5):
        self.asap_queue : deque[cm.InertCommand] = deque()
        self.retry_queue : deque[tuple[cm.InertCommand, float]] = deque()
        self.retry_interval = retry_interval

    def pop(self) -> tuple[cm.InertCommand | None, float | None]:
        """
        Returns the next command to send, if any, or the timepoint at which to
        try again
        """
        if self.asap_queue:
            return self.asap_queue.popleft(), None

        if self.retry_queue:
            next_cmd, next_time = self.retry_queue[0]
            now = time.time()
            if next_time > now:
                # Not time yet
                return None, next_time
            self.retry_queue.popleft()
            return next_cmd, None

        # Everything's empty
        return None, None

    def enqueue(self, command: cm.InertCommand):
        """
        Adds a command to the end of the ASAP queue
        """
        self.asap_queue.append(command)

    def requeue(self, command: cm.InertCommand):
        """
        Adds a command to the end of the retry queue, with a delay
        """
        self.retry_queue.append((
            command, time.time() + self.retry_interval
            ))
