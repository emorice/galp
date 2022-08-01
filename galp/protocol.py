"""
GALP protocol implementation
"""

import logging

import galp.graph
from galp.lower_protocol import LowerProtocol
from galp.eventnamespace import EventNamespace, NoHandlerError

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

    def on_done(self, route, name):
        """A `DONE` request was received for resource `name`"""
        return self.on_unhandled(b'DONE')

    def on_exit(self, route):
        """An `EXIT` message was received"""
        return self.on_unhandled(b'EXIT')

    def on_exited(self, route, peer):
        """An `EXITED` message was received"""
        return self.on_unhandled(b'EXITED')

    def on_failed(self, route, name):
        """A `FAILED` message was received"""
        return self.on_unhandled(b'FAILED')

    def on_get(self, route, name):
        """A `GET` request was received for resource `name`"""
        return self.on_unhandled(b'GET')

    def on_illegal(self, route, reason):
        """An `ILLEGAL` message was received"""
        return self.on_unhandled(b'ILLEGAL')

    def on_not_found(self, route, name):
        """A `NOTFOUND` message was received"""
        return self.on_unhandled(b'NOTFOUND')

    def on_put(self, route, name, serialized):
        """A `PUT` message was received for resource `name`
        """
        return self.on_unhandled(b'PUT')

    def on_ready(self, route, peer):
        """A `READY` message was received"""
        return self.on_unhandled(b'READY')

    def on_submit(self, route, name, task_dict):
        """A `SUBMIT` message was received"""
        return self.on_unhandled(b'SUBMIT')

    def on_unhandled(self, verb):
        """
        A message without an overriden callback was received.
        """
        logging.error("Unhandled GALP verb %s", verb.decode('ascii'))

    # Default handlers
    # ================
    def on_invalid(self, route, reason):
        """
        An invalid message was received.
        """
        raise IllegalRequestError(route, reason)

    # Send methods
    # ============
    def doing(self, route, name):
        """
        Builds a DOING message

        Args:
            name: the name of the task
        """
        return route, [b'DOING', name]

    def done(self, route, name):
        """
        Builds a DONE message

        Args:
            name: the name of the task
        """
        return route, [b'DONE', name]

    def exited(self, route, peer):
        """Signal the given peer has exited"""
        msg = [b'EXITED', peer]
        return route, msg

    def failed(self, route, name):
        """
        Builds a FAILED message

        Args:
            name: the name of the task
        """
        return route, [b'FAILED', name]

    def get(self, route, name):
        """
        Builds a GET message

        Args:
            name: the name of the task
        """
        msg = [b'GET', name]
        return route, msg

    def illegal(self, route, reason):
        """
        Builds an ILLEGAL message.

        This message is meant to be done when the peer has sent a message that
        should not have been send or contains a fundamental error. This is
        useful to trace back errors to the original sender but should not be
        needed in normal operation.
        """
        return route, [b'ILLEGAL', reason.encode('ascii')]

    def not_found(self, route, name):
        """
        Builds a NOTFOUND message

        Args:
            name: the name of the task
        """
        return route, [b'NOTFOUND', name]

    def put(self, route, name, serialized):
        """
        Builds a PUT message

        Args:
            name: the name of the task
            serialized: a triple of two bytes objects and an int (proto, data,
                children)

        """
        proto, data, children = serialized
        if data is None:
            data = b''
        logging.debug('-> putting %d bytes', len(data))
        msg_body = [b'PUT', name, proto, data, children.to_bytes(1, 'big')]
        return route, msg_body

    def ready(self, route, peer):
        """
        Sends a READY message.

        Args:
            peer: the self-assigned peer name. It can be anything provided that
               it's unique to the sender, and is usually chosen in a transparent
               way.
        """
        return route, [b'READY', peer]

    def submit_task(self, route, task):
        """Sends SUBMIT for given task object.

        Hereis-tasks and derived tasks should not be passed at all and will
        trigger an error, since they do not represent the result of a step and
        cannot be executed.

        Handle them in a wrapper or override.
        """

        if hasattr(task, 'hereis'):
            raise ValueError('Here-is tasks must never be passed to '
                'Protocol layer')
        if hasattr(task, 'parent'):
            raise ValueError('Derived tasks must never be passed to '
                'Protocol layer')

        task_dict = dict(
            step_name=task.step.key,
            vtags=task.vtags,
            arg_names=[ arg.name for arg in task.args ],
            kwarg_names={ kw: kwarg.name for kw, kwarg in task.kwargs.items() }
            )

        return self.submit(route, task_dict)

    def submit(self, route, task_dict):
        """
        Low-level submit routine.

        See also submit_task.

        Args:
            task_dict: dictionary containing all the serializable part of the
                task object to transmit. Contains the `step_name`, a list of
                `vtags`, a list of positional `arg_names` and a dictionary of
                `kwarg_names`.
        """
        msg = [b'SUBMIT', task_dict['step_name']]
        # Vtags
        vtags = task_dict['vtags']
        msg += [ len(vtags).to_bytes(1, 'big') ]
        for tag in vtags:
            msg += [ tag ]
        # Pos args
        for arg_name in task_dict['arg_names']:
            msg += [ b'', arg_name ]
        # Kw args
        for keyword, kwarg_name in task_dict['kwarg_names'].items():
            msg += [ keyword , kwarg_name ]
        return route, msg

    # Main logic methods
    # ==================
    def on_verb(self, route, msg_body):
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

    def handler(self, event_name):
        """Shortcut to call handler methods"""
        def _handler(*args, **kwargs):
            return self.event.handler(event_name)(self, *args, **kwargs)
        return _handler

    def send_illegal(self, route):
        """Send a straightforward error message back so that hell is raised where
        due.

        Note: this is technically part of the Protocol and should probably be
            moved there.
        """
        return self.illegal(route)

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

        name = msg[1]

        return self.on_get(route, name)

    @event.on('DOING')
    def _on_doing(self, route, msg):
        self._validate(len(msg) >= 2, route, 'DOING without a name')
        self._validate(len(msg) <= 2, route, 'DOING with too many names')

        name = msg[1]

        return self.on_doing(route, name)

    @event.on('DONE')
    def _on_done(self, route, msg):
        self._validate(len(msg) >= 2, route, 'DONE without a name')
        self._validate(len(msg) <= 2, route, 'DONE with too many names')

        name = msg[1]

        return self.on_done(route, name)

    @event.on('FAILED')
    def _on_done(self, route, msg):
        self._validate(len(msg) >= 2, route, 'FAILED without a name')
        self._validate(len(msg) <= 2, route, 'FAILED with too many names')

        name = msg[1]

        return self.on_failed(route, name)

    @event.on('NOTFOUND')
    def _on_not_found(self, route, msg):
        self._validate(len(msg) >= 2, route, 'NOTFOUND without a name')
        self._validate(len(msg) <= 2, route, 'NOTFOUND with too many names')

        name = msg[1]

        return self.on_not_found(route, name)

    @event.on('PUT')
    def _on_put(self, route, msg):
        self._validate(4 <= len(msg) <= 5, route, 'PUT with wrong number of parts')

        name = msg[1]
        proto = msg[2]
        data = msg[3]
        children = int.from_bytes(msg[4], 'big') if len(msg) >= 5 else 0

        return self.on_put(route, name, (proto, data, children))

    @event.on('READY')
    def _on_ready(self, route, msg):
        self._validate(len(msg) == 2, route, 'READY without exactly one peer id')
        peer = msg[1]
        return self.on_ready(route, peer)

    @event.on('SUBMIT')
    def _on_submit(self, route, msg):
        task_dict = {}

        self._validate(
            len(msg) >= 3, route,
            'SUBMIT without step or tag count') # SUBMIT step n_tags
        task_dict['step_name'] = msg[1]

        n_tags = int.from_bytes(msg[2], 'big')
        # Collect tags
        argstack = msg[3:]
        self._validate(
            len(argstack) >= n_tags, route,
            'Not as many tags as stated') # Expected number of tags
        task_dict['vtags'] = argstack[:n_tags]

        # Collect args
        argstack = argstack[n_tags:]
        argstack.reverse()
        task_dict['arg_names'] = []
        task_dict['kwarg_names'] = {}
        while argstack != []:
            try:
                keyword, arg_name = argstack.pop(), argstack.pop()
            except IndexError as exc:
                raise IllegalRequestError from exc
            if keyword == b'':
                task_dict['arg_names'].append(arg_name)
            else:
                task_dict['kwarg_names'][keyword] = arg_name

        name = galp.graph.Task.gen_name(task_dict)

        return self.on_submit(route, name, task_dict)
