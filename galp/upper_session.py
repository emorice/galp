"""
Core-layer writer
"""

from dataclasses import dataclass

from galp.messages import Message
from galp.writer import TransportMessage, Writer, add_frames
from galp.serializer import dump_model

def dump_message(message: Message) -> list[bytes]:
    """
    Serialize a message
    """
    return [message.message_get_key(), dump_model(message)]

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
