"""
GALP protocol implementation
"""

import logging

import galp.graph
from galp.eventnamespace import EventNamespace, NoHandlerError


class Protocol:
    """
    Helper class gathering methods solely concerned with parsing and building
    messages, but not with what to do with them.

    Methods named after a verb (`get`) build and send a message.
    Methods starting with `on_` and a verb (`on_get`) are called when such a
    message is recevived, and should usually be overriden unless the verb is to
    be ignored (with a warning).
    """

    # Callback methods
    # ================
    # The methods below are just placeholders that double as documentation.
    def on_get(self, route, name):
        """A `GET` request was received for resource `name`"""
        return self.on_unhandled(b'GET')

    def on_put(self, route, name, proto: bytes, data: bytes, children: int):
        """A `PUT` message was received for resource `name` 
        """
        return self.on_unhandled(b'PUT')

    def on_doing(self, route, name):
        """A `DOING` request was received for resource `name`"""
        return self.on_unhandled(b'DOING')

    def on_done(self, route, name):
        """A `DONE` request was received for resource `name`"""
        return self.on_unhandled(b'DONE')

    def on_failed(self, route, name):
        """A `FAILED` message was received"""
        return self.on_unhandled(b'FAILED')

    def on_not_found(self, route, name):
        """A `NOTFOUND` message was received"""
        return self.on_unhandled(b'NOTFOUND')

    def on_exit(self, route):
        """An `EXIT` message was received"""
        return self.on_unhandled(b'EXIT')

    def on_illegal(self, route):
        """An `ILLEGAL` message was received"""
        return self.on_unhandled(b'ILLEGAL')

    def on_invalid(self, route, reason):
        """
        An invalid message was received.
        """
        logging.warning("Invalid message but no handler: %s", reason)

    def on_unhandled(self, verb):
        """
        A message without an overriden callback was received.
        """
        logging.warning("Unhandled GALP verb %s", verb.decode('ascii'))
        return False # Not fatal by default


    # Send methods
    # ============
    def get(self, route, task):
        """Send get for task with given name"""
        msg = [b'GET', task]
        return self._send_message(route, msg)

    def put(self, route, name, proto, data, children):
        if data is None:
            data = b''
        logging.debug('-> putting %d bytes', len(data))
        m = [b'PUT', name, proto, data, children]
        return self._send_message(route, m)

    def not_found(self, route, name):
        return self._send_message(route, [b'NOTFOUND', name])

    def submit(self, route, task):
        """Send submit for given task object.

        Hereis-tasks should not be passed at all and will trigger an error.
        Handle them in a wrapper or override.
        """

        if hasattr(task, 'hereis'):
            raise ValueError('Here-is tasks must never be passed to '
                'Protocol layer')
        if hasattr(task, 'parent'):
            raise ValueError('Derived tasks must never be passed to '
                'Protocol layer')

        # Step
        msg = [b'SUBMIT', task.step.key]
        # Vtags
        msg += [ len(task.vtags).to_bytes(1, 'big') ]
        for tag in task.vtags:
            msg += [ tag ]
        # Pos args
        for arg in task.args:
            msg += [ b'', arg.name ]
        # Kw args
        for kw, kwarg in task.kwargs.items():
            msg += [ kw , kwarg.name ]
        return self._send_message(route, msg)

    def done(self, route, name):
        return self._send_message(route, [b'DONE', name])

    def doing(self, route, name):
        return self._send_message(route, [b'DOING', name])

    def failed(self, route, name):
        return self._send_message(route, [b'FAILED', name])

    def illegal(self, route):
        return self._send_message(route, [b'ILLEGAL'])

    # Main logic methods
    # ==================
    def on_message(self, msg):
        """Parse given message, calling callbacks as needed.

        Returns:
            Whatever the final handler for this message returned. Most
            importantly, if you use asynchronous handlers, this will return the
            coroutine to await. Also note that by default it return 'False' for
            unhandled messages (not None, since this would not allow to
            distinguish a message that was not handled from one that was handled
            without a return value).

        Note: handler for invalid incoming messages does not follow this scheme,
            as an invalid incoming message is never interpreted as a normal end of
            communication. Raise exceptions from the handler if you want to
            e.g. abort on invalid messages. On the other hand, returning a stop
            condition on a 'ILLEGAL' message works, since it is a regular
            message that signals a previous error.
        """
        self.validate(len(msg) > 0, 'Empty message')
        msg, route = self.get_routing_parts(msg)
        logging.info('<- [%s] %s', route[0].hex() if route else "", msg[0].decode('ascii'))
        try:
            return self.handler(str(msg[0], 'ascii'))(route, msg)
        except NoHandlerError:
            self.validate(False, route, 'No such verb')
        return False

    def get_routing_parts(self, msg):
        """
        Parses and returns the routing part of `msg`, and the rest.

        Can be overloaded to handle different routing strategies.
        """
        route = []
        while msg and msg[0]:
            route.append(msg[0])
            msg = msg[1:]
        # Discard empty frame
        msg = msg[1:]
        self.validate(len(msg) > 0, route, 'Empty message with only routing information')
        logging.debug("%d routing part(s)", len(route))
        return msg, route


    def send_message(self, msg):
        """Callback method to send a message just built.

        This is the only callback that is required to be implemented.
        """
        raise NotImplementedError("You must override the send_message method "
            "when subclassing a Protocol object.")

    def _send_message(self, route, msg):
        """Private callback method to send a message just built.

        Wrapper around send_message that concats route and message.
        """
        logging.info('-> [%s] %s', route[0].hex() if route else "", msg[0].decode('ascii'))
        return self.send_message(route + [b''] + msg)

    # Internal message handling methods
    event = EventNamespace()

    def handler(self, event_name):
        """Shortcut to call handler methods"""
        def _handler(*args, **kwargs):
            return self.event.handler(event_name)(self, *args, **kwargs)
        return _handler

    def validate(self, condition, route, reason='Unknown error'):
        if not condition:
            self.on_invalid(route, reason)

    @event.on('EXIT')
    def _on_exit(self, route, msg):
        self.validate(len(msg) == 1, route, 'EXIT with args')
        return self.on_exit()

    @event.on('ILLEGAL')
    def _on_illegal(self, route, msg):
        self.validate(len(msg) == 1, route, 'ILLEGAL with args')
        return self.on_illegal()

    @event.on('GET')
    def _on_get(self, route, msg):
        self.validate(len(msg) >= 2, route, 'GET without a name')
        self.validate(len(msg) <= 2, route, 'GET with too many names')

        name = msg[1]

        return self.on_get(route, name)

    @event.on('DOING')
    def _on_doing(self, route, msg):
        self.validate(len(msg) >= 2, route, 'DOING without a name')
        self.validate(len(msg) <= 2, route, 'DOING with too many names')

        name = msg[1]

        return self.on_doing(name)

    @event.on('DONE')
    def _on_done(self, route, msg):
        self.validate(len(msg) >= 2, route, 'DONE without a name')
        self.validate(len(msg) <= 2, route, 'DONE with too many names')

        name = msg[1]

        return self.on_done(name)

    @event.on('FAILED')
    def _on_done(self, route, msg):
        self.validate(len(msg) >= 2, route, 'FAILED without a name')
        self.validate(len(msg) <= 2, route, 'FAILED with too many names')

        name = msg[1]

        return self.on_failed(name)

    @event.on('NOTFOUND')
    def _on_not_found(self, route, msg):
        self.validate(len(msg) >= 2, route, 'NOTFOUND without a name')
        self.validate(len(msg) <= 2, route, 'NOTFOUND with too many names')

        name = msg[1]

        return self.on_not_found(name)

    @event.on('PUT')
    def _on_put(self, route, msg):
        self.validate(4 <= len(msg) <= 5, route, 'PUT with wrong number of parts')

        name = msg[1]
        proto = msg[2]
        data = msg[3]
        children = int.from_bytes(msg[4], 'big') if len(msg) >= 5 else 0

        return self.on_put(route, name, proto, data, children)

    @event.on('SUBMIT')
    def _on_submit(self, route, msg):
        self.validate(len(msg) >= 3, route, 'SUBMIT without step or tag count') # SUBMIT step n_tags

        step_name = msg[1]
        n_tags = int.from_bytes(msg[2], 'big')

        # Collect tags
        argstack = msg[3:]
        self.validate(len(argstack) >= n_tags, route, 'Not as many tags as stated') # Expected number of tags
        vtags, argstack = argstack[:n_tags], argstack[n_tags:]

        # Collect args
        argstack.reverse()
        arg_names = []
        kwarg_names = {}
        while argstack != []:
            try:
                keyword, arg_name = argstack.pop(), argstack.pop()
            except IndexError:
                raise IllegalRequestError
            if keyword == b'':
                arg_names.append(arg_name)
            else:
                kwarg_names[keyword] = arg_name

        name = galp.graph.Task.gen_name(step_name, arg_names, kwarg_names, vtags)

        return self.on_submit(route, name, step_name, arg_names, kwarg_names)
