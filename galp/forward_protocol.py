"""
Galp protocol that returns the original message after calling handlers
"""

from galp.protocol import Protocol

class ForwardProtocol(Protocol):
    """
    Protocol returning original message after calling handlers.
    """
    def on_verb(self, route, msg_body):
        """
        Message handler that call's Protocol default handler
        but returns the original message when the handler generates no message.

        If the handler generates a message, it replaces the original and gets
        forwarded instead.
        """
        reply = super().on_verb(route, msg_body)
        if reply:
            return reply
        return (route, msg_body)
