"""
Writer objects related to routing information

Since we don't need that much modularity this directly import the core layer
writer, but should work with any layer.
"""

from dataclasses import dataclass

from galp.net.core.dump import Writer, Message, dump_message

# Routing-layer data structures
# =============================

Route = list[bytes]
"""
This actually ZMQ specific and should be changed to a generic if we ever need
more protocols
"""

@dataclass
class Routes:
    """
    The routing layer of a message
    """
    incoming: Route
    forward: Route

# Routing-layer writers
# =====================

def dump_routes(is_router: bool, routes: Routes) -> list[bytes]:
    """Serializes routes"""
    if is_router:
        # If routing, we need an id, we take it from the forward segment
        # Note: this id is consumed by the zmq router sending stage and does
        # not end up on the wire
        next_hop, *forward_route = routes.forward
        return [next_hop, *routes.incoming, *forward_route, b'']
    return [*routes.incoming, *routes.forward, b'']

def make_message_writer(write: Writer[list[bytes]],
        is_router: bool, routes: Routes) -> Writer[Message]:
    """Serialize and write routes and core message"""
    return lambda msg: write(dump_routes(is_router, routes) + dump_message(msg))

@dataclass
class ReplyFromSession:
    """
    Session encapsulating a destination but letting the user fill in the origin
    part of the message

    This is what is exposed to the user, and must be type safe.
    """
    write_lower: Writer[list[bytes]]
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
        return make_message_writer(self.write_lower, self.is_router,
                Routes(incoming=nat_origin, forward=self.forward)
                )

    @property
    def uid(self):
        """
        Hashable identifier for this destination
        """
        return tuple(self.forward)

@dataclass
class ForwardSessions:
    """
    Pair of sessions exposed to app on forward.

    The two sessions represent the sender of the message, and its recipient
    """
    origin: ReplyFromSession
    dest: ReplyFromSession

def make_local_writer(is_router: bool) -> Writer[Message]:
    """
    Create a default-addressing session
    """
    return make_message_writer(lambda msg: msg, is_router,
            Routes(incoming=Route(), forward=Route())
            )
