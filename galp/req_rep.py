"""
Request-reply pattern with multiplexing
"""

from galp.net.core.types import Reply
from galp.commands import Script, InertCommand

def handle_reply(msg: Reply, script: Script) -> list[InertCommand]:
    """
    Handle a reply by fulfilling promise and calling callbacks, return new
    promises
    """
    return script.done(msg.request.as_legacy_key(), msg.value)
