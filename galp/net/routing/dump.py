"""
Writer objects related to routing information

Since we don't need that much modularity this directly import the core layer
writer, but should work with any layer.
"""

from typing import TypeAlias
from dataclasses import dataclass

from galp.net.core.dump import Writer, Message, dump_message
from .types import Routed, Route

def write_local(message: Message) -> list[bytes]:
    """
    Add trivial routing frame for message with neither source nor dest
    """
    return [b'', *dump_message(message)]

def dump_routed(is_router: bool, routed: Routed) -> list[bytes]:
    """Serializes routes"""
    if is_router:
        # If routing, we need an id, we take it from the forward segment
        # Note: this id is consumed by the zmq router sending stage and does
        # not end up on the wire
        next_hop, *forward_route = routed.forward
        return [next_hop, *routed.incoming, *forward_route, b'', *dump_message(routed.body)]
    return [*routed.incoming, *routed.forward, b'', *dump_message(routed.body)]

def make_message_writer(
        is_router: bool, incoming: Route, forward: Route) -> Writer[Message]:
    """Serialize and write routes and core message"""
    return lambda msg: dump_routed(is_router,
        Routed(incoming, forward, msg)
        )

SessionUid: TypeAlias = tuple[bytes, ...]
"""
Alias to the hashable type used to identify sessions
"""

@dataclass
class ReplyFromSession:
    """
    Session encapsulating a destination but letting the user fill in the origin
    part of the message

    This is what is exposed to the user, and must be type safe. It is hashable
    and comparable, such that sessions obtained from the same peer on different
    messages compare equal.

    This comparability does not check anything concerning the layer below
    routing. Concretely, don't try to compare sessions built on top of different
    zmq sockets.
    """
    is_router: bool
    forward: Route

    def reply_from(self, origin: 'ReplyFromSession | None') -> Writer[Message]:
        """
        Creates a session to send galp messages

        Args:
            origin: where the message originates from and where replies should
            be sent. If None, means the message was locally generated.
        """
        nat_origin = Route() if origin is None else origin.forward
        return make_message_writer(self.is_router,
                incoming=nat_origin, forward=self.forward)

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

@dataclass
class ForwardSessions:
    """
    Pair of sessions exposed to app on forward.

    The two sessions represent the sender of the message, and its recipient
    """
    origin: ReplyFromSession
    dest: ReplyFromSession

def make_local_writer(is_router: bool = False) -> Writer[Message]:
    """
    Create a default-addressing session
    """
    return make_message_writer(is_router, incoming=Route(), forward=Route())
