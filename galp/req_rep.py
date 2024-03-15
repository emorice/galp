"""
Request-reply pattern with multiplexing
"""

import logging

from galp.net.core.types import Reply
from galp.commands import Script, InertCommand

def handle_reply(msg: Reply, script: Script) -> list[InertCommand]:
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
    return command.done(msg.value)
