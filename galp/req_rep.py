"""
Request-reply pattern with multiplexing
"""

import logging
from typing import Callable, TypeAlias

from galp.result import Error
import galp.net.requests.types as gr
from galp.net.core.types import Reply, ReplyValue
from galp.commands import Script, PrimitiveProxy, InertCommand

ReplyHandler: TypeAlias = Callable[[PrimitiveProxy, ReplyValue], list[InertCommand]]

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
    # Handle remote failures
    maybe_value = msg.value
    if isinstance(maybe_value, gr.RemoteError):
        command.failed(f'RemoteError: {maybe_value.error}')
        return []
    value = maybe_value.value
    # Try dispatch based on request verb:
    match verb:
        case 'STAT':
            return _on_stat_reply(command, value)
        case 'GET':
            return _on_get_reply(command, value)
        case 'SUBMIT':
            return _on_submit_reply(command, value)
    logging.error('No handler for reply to %s', verb)
    return []

# Reply adapters
# ==============
# Probably not needed on the long run

def _on_stat_reply(stat_command, msg: gr.Found | gr.StatDone | gr.NotFound) -> list[InertCommand]:
    """Handle stat replies"""
    return stat_command.done(msg)

def _on_get_reply(get_command, msg: gr.Put) -> list[InertCommand]:
    """
    Handle get replies
    """
    return get_command.done(msg)

def _on_submit_reply(sub_command, msg: gr.Done | gr.Failed) -> list[InertCommand]:
    """
    Handle submit replies
    """
    match msg:
        case gr.Done():
            return sub_command.done(msg.result)
        case gr.Failed():
            task_def = msg.task_def
            err_msg = f'Failed to execute task {task_def}, check worker logs'
            logging.error('TASK FAILED: %s', err_msg)

            # Mark fetch command as failed if pending
            return sub_command.failed(Error(err_msg))
