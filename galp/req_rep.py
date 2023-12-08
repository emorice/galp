"""
Request-reply pattern with multiplexing
"""

from galp.protocol import (DispatchFunction, Handler, TransportMessage,
    UpperSession)
from galp.messages import Reply

def make_reply_handler(dispatch: DispatchFunction):
    """
    Make handler for all replies
    """
    def _on_reply(session: UpperSession, msg: Reply) -> list[TransportMessage]:
        """
        Handle a reply
        """
        # Here will be the code to generally extract the promise
        # ...
        # Dispatch
        return dispatch(session, msg.value)
    return Handler(Reply, _on_reply)
