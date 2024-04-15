"""
Parsing for routing layer
"""

from galp.net.base.load import LoadError
from galp.result import Ok

from galp.net.core.load import parse_core_message
from .types import Routed

def load_routed(msg: list[bytes]) -> Ok[Routed] | LoadError:
    """
    Parses and returns the routing part of `msg`, and body.
    """
    route_parts = []
    while msg and msg[0]:
        route_parts.append(msg[0])
        msg = msg[1:]
    # Whatever was found is treated as the route. If it's malformed, we
    # cannot know, and we cannot send answers anywhere else.
    incoming, forward = route_parts[:1], route_parts[1:]

    # Discard empty frame
    if not msg or  msg[0]:
        return LoadError('Missing empty delimiter frame')

    return parse_core_message(msg[1:]).then(
            lambda core: Ok(Routed(
                incoming=incoming, forward=forward,
                body=core
                ))
            )
