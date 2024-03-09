"""
Queue of active commands
"""
from collections import deque

import galp.commands as cm

class CommandQueue:
    """
    A queue of active commands that need periodic re-issuing
    """
    def __init__(self) -> None:
        self.asap_queue : deque[cm.InertCommand] = deque()
        self.retry_queue : deque[cm.InertCommand] = deque()
        self.can_send: bool = True

    def pop(self) -> cm.InertCommand | None:
        """
        Returns the next command to send, if any, or the timepoint at which to
        try again
        """
        # If remote is blocking, don't send anything
        if not self.can_send:
            return None

        if self.asap_queue:
            self.can_send = False
            return self.asap_queue.popleft()

        # FIXME: with retry disabled, we're leaking memory because we never empty
        # retry queue
        # if self.retry_queue:
        #     next_cmd, next_time = self.retry_queue[0]
        #     now = time.time()
        #     if next_time > now:
        #         # Not time yet
        #         return None, next_time
        #     self.retry_queue.popleft()
        #     return next_cmd, None

        # Everything's empty
        return None

    def on_next_request(self) -> cm.InertCommand | None:
        """Returns next command to send, no matter the time left"""
        if self.asap_queue:
            return self.asap_queue.popleft()
        # If we fail a pop, remember we can send the next one right through
        self.can_send = True
        return None

    def enqueue(self, command: cm.InertCommand):
        """
        Adds a command to the end of the ASAP queue
        """
        self.asap_queue.append(command)

    def requeue(self, command: cm.InertCommand):
        """
        Adds a command to the end of the retry queue
        """
        self.retry_queue.append(command)
