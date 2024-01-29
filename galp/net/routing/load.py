"""
Parsing for routing layer
"""

from galp.net.base.load import LoadError

from .types import Routes

def load_routes(msg: list[bytes]) -> tuple[Routes, list[bytes]] | LoadError:
    """
    Parses and returns the routing part of `msg`, and body.

    Can be overloaded to handle different routing strategies.
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
    payload = msg[1:]

    return Routes(incoming=incoming, forward=forward), payload
