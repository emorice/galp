"""
Routing-layer data structures
"""

from dataclasses import dataclass

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
