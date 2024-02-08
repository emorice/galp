"""
Request-reply pattern with multiplexing
"""

import logging
from typing import Callable, TypeAlias

from galp.net.core.types import Reply, ReplyValue
from galp.commands import Script, PrimitiveProxy, InertCommand

ReplyHandler: TypeAlias = Callable[[PrimitiveProxy, ReplyValue], list[InertCommand]]

def handle_reply(msg: Reply, script: Script, handlers: dict[str, ReplyHandler]
        ) -> list[InertCommand]:
    """
    Handle a reply by fulfilling promise and calling callbacks, return new
    promises
    """
    # Extract the promise
    verb = msg.request.verb.decode('ascii').upper()
    promise_id = (verb, msg.request.name)
    command = script.commands.get(promise_id)
    if not command:
        logging.error('Dropping answer to missing promise %s', promise_id)
        return []
    # Try dispatch based on request verb:
    if verb in handlers:
        return handlers[verb](command, msg.value)
    logging.error('No handler for reply to %s', verb)
    return []
