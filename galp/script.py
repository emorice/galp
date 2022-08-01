"""
Galp script
"""

# subkeys
OVER = 0
FAILED = 1
DONE = 2

class Script:
    """
    A set of commands with dependencies between them.

    It's essentially a collection of futures, but indexed by an application key,
    and with conveniences to express complicated trigger conditions more easily.
    """
    def __init__(self):
        self._over = {} # defaults to false
        self._status = {}
        self._downstream = {}
        self._triggers = {}
        self._triggered = {}
        self._callbacks = {}

    def status(self, flag):
        """
        Check if a command is known to be in the given state
        """
        command_key, state = flag

        if state == OVER:
            return (
                self.status((command_key, DONE))
                or self.status((command_key, FAILED))
                )
        return self._status[flag]

    def done(self, command_key):
        """
        Marks the corresponding command as successfully done, and runs callbacks
        synchronously
        """

        # First, we need to write the status of the command since further
        # sister command may need it later
        self._status[command_key, DONE] = True

        # Next, we need to check for downstream commands
        for down_key in self._downstream[command_key]:
            self._maybe_trigger(down_key)

    def _maybe_trigger(self, command_key):
        """
        Check for trigger conditions and call the callback at most once
        """
        # Reentrancy check
        if self._triggered[command_key]:
            return

        if all(
            self.status(flag)
            for flag in self._triggers[command_key]
            ):
            self._callbacks[command_key, DONE]()


