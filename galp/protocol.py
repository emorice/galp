"""
GALP protocol implementation
"""

import logging

from galp.task_types import (TaskName, NamedTaskDef, CoreTaskDef,
                             NamedCoreTaskDef, is_core)
from galp.lower_protocol import LowerProtocol
from galp.eventnamespace import EventNamespace, NoHandlerError
from galp.serializer import load_task_def, load_core_task_def, dump_task_def

# Errors and exceptions
# =====================
class ProtocolEndException(Exception):
    """
    Exception thrown by a handler to signal that no more messages are expected
    and the transport should be closed
    """
class IllegalRequestError(Exception):
    """Base class for all badly formed requests, triggers sending an ILLEGAL
    message back"""
    def __init__(self, route, reason):
        super().__init__()
        self.route = route
        self.reason = reason

# High-level protocol
# ===================
class Protocol(LowerProtocol):
    """
    Helper class gathering methods solely concerned with parsing and building
    messages, but not with what to do with them.

    Methods named after a verb (`get`) build and send a message.
    Methods starting with `on_` and a verb (`on_get`) are called when such a
    message is received, and should usually be overriden unless the verb is to
    be ignored (with a warning).
    """
    # FIXME: this is a temp disable, I'm not set on what's the correct way
    # pylint: disable=unused-argument
    # Callback methods
    # ================
    # The methods below are just placeholders that double as documentation.
    def on_doing(self, route, name):
        """A `DOING` request was received for resource `name`"""
        return self.on_unhandled(b'DOING')

    def on_done(self, route, named_def: NamedTaskDef, children: list[TaskName]):
        """A `DONE` request was received for resource `name`"""
        return self.on_unhandled(b'DONE')

    def on_exit(self, route):
        """An `EXIT` message was received"""
        return self.on_unhandled(b'EXIT')

    def on_exited(self, route, peer):
        """An `EXITED` message was received"""
        return self.on_unhandled(b'EXITED')

    def on_failed(self, route, name: TaskName):
        """A `FAILED` message was received"""
        return self.on_unhandled(b'FAILED')

    def on_get(self, route, name: TaskName):
        """A `GET` request was received for resource `name`"""
        return self.on_unhandled(b'GET')

    def on_illegal(self, route, reason: bytes):
        """An `ILLEGAL` message was received"""
        return self.on_unhandled(b'ILLEGAL')

    def on_not_found(self, route, name: TaskName):
        """A `NOTFOUND` message was received"""
        return self.on_unhandled(b'NOTFOUND')

    def on_put(self, route, name: TaskName, serialized: tuple[bytes, list[TaskName]]):
        """A `PUT` message was received for resource `name`
        """
        return self.on_unhandled(b'PUT')

    def on_ready(self, route, peer: bytes):
        """A `READY` message was received"""
        return self.on_unhandled(b'READY')

    def on_submit(self, route, named_def: NamedCoreTaskDef):
        """A `SUBMIT` message was received"""
        return self.on_unhandled(b'SUBMIT')

    def on_found(self, route, named_def: NamedTaskDef):
        """A `FOUND` message was received"""
        return self.on_unhandled(b'FOUND')

    def on_stat(self, route, name: TaskName):
        """A `STAT` message was received"""
        return self.on_unhandled(b'STAT')

    def on_unhandled(self, verb: bytes):
        """
        A message without an overriden callback was received.
        """
        logging.error("Unhandled GALP verb %s", verb.decode('ascii'))

    # Default handlers
    # ================
    def on_invalid(self, route, reason: str):
        """
        An invalid message was received.
        """
        raise IllegalRequestError(route, reason)

    # Send methods
    # ============
    def doing(self, route, name: TaskName):
        """
        Builds a DOING message

        Args:
            name: the name of the task
        """
        return route, [b'DOING', name]

    def done(self, route, named_def: NamedTaskDef, children: list[TaskName]):
        """
        Builds a DONE message

        Args:
            name: the name of the task
        """
        name, payload = dump_task_def(named_def)
        return route, [b'DONE', name, payload, *children]

    def exited(self, route, peer: bytes):
        """Signal the given peer has exited"""
        msg = [b'EXITED', peer]
        return route, msg

    def failed(self, route, name: TaskName):
        """
        Builds a FAILED message

        Args:
            name: the name of the task
        """
        return route, [b'FAILED', name]

    def get(self, route, name: TaskName):
        """
        Builds a GET message

        Args:
            name: the name of the task
        """
        msg = [b'GET', name]
        return route, msg

    def illegal(self, route, reason: str):
        """
        Builds an ILLEGAL message.

        This message is meant to be done when the peer has sent a message that
        should not have been send or contains a fundamental error. This is
        useful to trace back errors to the original sender but should not be
        needed in normal operation.
        """
        return route, [b'ILLEGAL', reason.encode('ascii')]

    def not_found(self, route, name: TaskName):
        """
        Builds a NOTFOUND message

        Args:
            name: the name of the task
        """
        return route, [b'NOTFOUND', name]

    def put(self, route, name, serialized: tuple[bytes, list[TaskName]]):
        """
        Builds a PUT message

        Args:
            name: the name of the task
            serialized: a tuple of a bytes object, the payload, and a list of
                bytes objects, the names of the sub-objects needed.

        """
        data, children = serialized
        logging.debug('-> putting %d bytes', len(data))
        msg_body = [b'PUT', name, data, *children]
        return route, msg_body

    def ready(self, route, peer: bytes):
        """
        Sends a READY message.

        Args:
            peer: the self-assigned peer name. It can be anything provided that
               it's unique to the sender, and is usually chosen in a transparent
               way.
        """
        return route, [b'READY', peer]


    def submit(self, route, named_def: NamedCoreTaskDef):
        """Sends SUBMIT for given task object.

        Literal and derived tasks should not be passed at all and will trigger
        an error, since they do not represent the result of a step and cannot be
        executed.

        Handle them in a wrapper or override.
        """
        if not is_core(named_def):
            raise ValueError('Only core tasks can be passed to Protocol layer')

        name, payload = dump_task_def(named_def)

        return route, [b'SUBMIT', name, payload]

    def stat(self, route, name: TaskName):
        """
        Builds a STAT message

        Args:
            name: the name of the task
        """
        return route, [b'STAT', name]

    def found(self, route, named_def: NamedTaskDef):
        """
        Builds a FOUND message
        """
        name, payload = dump_task_def(named_def)

        return route, [b'FOUND', name, payload]

    # Main logic methods
    # ==================
    def on_verb(self, route, msg_body: list[bytes]):
        """Parse given message, calling callbacks as needed.

        Returns:
            Whatever the final handler for this message returned.
        """
        str_verb = str(msg_body[0], 'ascii')
        try:
            return self.handler(str_verb)(route, msg_body)
        except NoHandlerError:
            self._validate(False, route, f'No such verb "{str_verb}"')
        return None


    # Internal message handling methods
    event = EventNamespace()

    def handler(self, event_name: str):
        """Shortcut to call handler methods"""
        def _handler(*args, **kwargs):
            return self.event.handler(event_name)(self, *args, **kwargs)
        return _handler

    @event.on('EXIT')
    def _on_exit(self, route, msg):
        self._validate(len(msg) == 1, route, 'EXIT with args')
        return self.on_exit(route)

    @event.on('EXITED')
    def _on_exited(self, route, msg):
        self._validate(len(msg) == 2, route, 'EXITED without exactly one peer id')
        peer = msg[1]
        return self.on_exited(route, peer)

    @event.on('ILLEGAL')
    def _on_illegal(self, route, msg):
        self._validate(len(msg) >= 2, route, 'ILLEGAL without a reason')
        self._validate(len(msg) <= 2, route, 'ILLEGAL with too many reasons')

        reason = msg[1]

        return self.on_illegal(route, reason)

    @event.on('GET')
    def _on_get(self, route, msg):
        self._validate(len(msg) >= 2, route, 'GET without a name')
        self._validate(len(msg) <= 2, route, 'GET with too many names')

        name = TaskName(msg[1])

        return self.on_get(route, name)

    @event.on('DOING')
    def _on_doing(self, route, msg):
        self._validate(len(msg) >= 2, route, 'DOING without a name')
        self._validate(len(msg) <= 2, route, 'DOING with too many names')

        name = TaskName(msg[1])

        return self.on_doing(route, name)

    @event.on('DONE')
    def _on_done(self, route, msg):
        self._validate(len(msg) >= 3, route, 'DONE without name or definition')

        named_def = load_task_def(name=msg[1], def_buffer=msg[2])
        children = [ TaskName(child_name) for child_name in msg[3:] ]

        return self.on_done(route, named_def, children)

    @event.on('FAILED')
    def _on_failed(self, route, msg):
        self._validate(len(msg) >= 2, route, 'FAILED without a name')
        self._validate(len(msg) <= 2, route, 'FAILED with too many names')

        name = TaskName(msg[1])

        return self.on_failed(route, name)

    @event.on('NOTFOUND')
    def _on_not_found(self, route, msg):
        self._validate(len(msg) >= 2, route, 'NOTFOUND without a name')
        self._validate(len(msg) <= 2, route, 'NOTFOUND with too many names')

        name = TaskName(msg[1])

        return self.on_not_found(route, name)

    @event.on('PUT')
    def _on_put(self, route, msg):
        # PUT name data [children]
        self._validate(3 <= len(msg), route, 'PUT with wrong number of parts')

        name = TaskName(msg[1])
        data = msg[2]
        children = [ TaskName(child_name) for child_name in msg[3:] ]

        return self.on_put(route, name, (data, children))

    @event.on('READY')
    def _on_ready(self, route, msg):
        self._validate(len(msg) == 2, route, 'READY without exactly one peer id')
        peer = msg[1]
        return self.on_ready(route, peer)

    @event.on('SUBMIT')
    def _on_submit(self, route, msg: list[bytes]):
        self._validate(len(msg) == 3, route, 'SUBMIT with wrong number of parts')
        named_def = load_core_task_def(name=msg[1], def_buffer=msg[2])
        return self.on_submit(route, named_def)

    @event.on('FOUND')
    def _on_found(self, route, msg):
        self._validate(len(msg) == 3, route, 'FOUND with wrong number of parts')
        named_def = load_task_def(name=msg[1], def_buffer=msg[2])
        return self.on_found(route, named_def)

    @event.on('STAT')
    def _on_stat(self, route, msg):
        self._validate(len(msg) >= 2, route, 'STAT without a name')
        self._validate(len(msg) <= 2, route, 'STAT with too many names')

        name = TaskName(msg[1])

        return self.on_stat(route, name)
