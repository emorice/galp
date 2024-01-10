"""
Core-layer writer
"""

from dataclasses import dataclass

from galp.messages import BaseMessage
from galp.writer import TransportMessage, Writer
from galp.serializer import dump_model

@dataclass
class UpperSession:
    """
    Session encapsulating a galp message to be sent
    """
    write_lower: Writer

    def write(self, message: BaseMessage) -> TransportMessage:
        """
        Write a complete message from a galp message object.

        Route is specified through the lower_session attribute.
        """
        frames = [
                dump_model(message, exclude={'data'})
                ]
        if hasattr(message, 'data'):
            frames.append(message.data)
        return self.write_lower(frames)
