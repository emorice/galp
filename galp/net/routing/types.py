"""
Routing-layer data structures
"""

from dataclasses import dataclass

from galp.net.core.types import Message

Route = list[bytes]
"""
This actually ZMQ specific and should be changed to a generic if we ever need
more protocols
"""

@dataclass
class Routed:
    """
    The routing layer of a message
    """
    incoming: Route
    forward: Route
    body: Message
