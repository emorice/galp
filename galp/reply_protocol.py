"""
Galp protocol for leaf peers
"""

import galp.messages as gm
from galp.protocol import Protocol, IllegalRequestError

class ReplyProtocol(Protocol):
    """
    Protocol for leaf peers.

    Currently, the only feature is to catch IllegalRequest exceptions and
    generate the illegal message as the answer. This way the handling of
    malformed message is correct when used with the default listen-reply loop.
    """
    def on_verb(self, route, msg_body):
        """
        Message handler that call's Protocol default handler
        but returns the original message
        """
        try:
            return super().on_verb(route, msg_body)
        except IllegalRequestError as exc:
            return gm.Illegal.plain_reply(exc.route, reason=exc.reason)
