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
        but returns the original message
        """
        reply = super().on_verb(route, msg_body)
        if reply:
            raise ValueError(f"Forwarding protocol handlers cannot generate "
                f"replies (verb was \"{msg_body[:1]}\")"
                )
        return (route, msg_body)
