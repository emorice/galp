"""
Writer objects related to routing information

Since we don't need that much modularity this directly import the core layer
writer, but should work with any layer.
"""

from typing import TypeAlias
from dataclasses import dataclass

from galp.net.core.dump import Writer, Message, dump_message
from .types import Route

def write_local(message: Message) -> list[bytes]:
    """
    Add trivial routing frame for message with neither source nor dest
    """
    return [b'', *dump_message(message)]

SessionUid: TypeAlias = tuple[bytes, ...]
"""
Alias to the hashable type used to identify sessions
"""

@dataclass
class ReplyFromSession:
    """
    Session encapsulating a destination.

    This is what is exposed to the user, and must be type safe. It is hashable
    and comparable, such that sessions obtained from the same peer on different
    messages compare equal.

    This comparability does not check anything concerning the layer below
    routing. Concretely, don't try to compare sessions built on top of different
    sockets.
    """
    forward: Route

    def reply(self) -> Writer[Message]:
        """
        Creates a session to send galp messages

        Args:
            origin: where the message originates from and where replies should
            be sent. If None, means the message was locally generated.
        """
        return lambda message: [*self.forward, b'', *dump_message(message)]

    @property
    def uid(self) -> SessionUid:
        """
        Hashable identifier for this destination
        """
        return tuple(self.forward)

    def __hash__(self) -> int:
        return hash(self.uid)

    def __eq__(self, other) -> bool:
        if isinstance(other, self.__class__):
            return self.uid == other.uid
        return NotImplemented
