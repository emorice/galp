"""
Galp protocol for leaf peers
"""

from typing import Iterable

import galp.messages as gm
from galp.lower_protocol import IllegalRequestError, Session
from galp.protocol import Protocol, Route, RoutedMessage

class ReplyProtocol(Protocol):
    """
    Protocol for leaf peers.

    Currently, the only feature is to catch IllegalRequest exceptions and
    generate the illegal message as the answer. This way the handling of
    malformed message is correct when used with the default listen-reply loop.
    """
    def on_verb(self, session: Session, route: tuple[Route, Route], msg_body: list[bytes]
            ) -> Iterable[RoutedMessage]:
        """
        Message handler that call's Protocol default handler
        and catches IllegalRequestError
        """
        try:
            return super().on_verb(session, route, msg_body)
        # Obsolete pathway
        except IllegalRequestError as exc:
            return [self.write_message(RoutedMessage(
                    incoming=Route(),
                    forward=route[0],
                    body=gm.Illegal(reason=exc.reason)
                    ))]
