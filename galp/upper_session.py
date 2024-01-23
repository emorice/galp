"""
Core-layer writer
"""

from dataclasses import dataclass

from galp.net.core.types import Message
from galp.net.core.dump import dump_message
from galp.writer import TransportMessage, Writer, add_frames

# Core message session
# --------------------

@dataclass
class UpperSession:
    """
    Session encapsulating a galp message to be sent
    """
    write_lower: Writer

    def write(self, message: Message) -> TransportMessage:
        """
        Write a complete message from a galp message object.

        Route is specified through the lower_session attribute.
        """
        return add_frames(
                self.write_lower, dump_message(message)
                )([])
