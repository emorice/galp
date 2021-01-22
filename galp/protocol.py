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
    async def on_get(self, name):
        """A `GET` request was received for resource `name`"""
        return self.on_unhandled(b'GET')

    async def on_put(self, name, obj):
        """A `PUT` message was received for resource `name` with object `obj`"""
        return self.on_unhandled(b'PUT')

    async def on_doing(self, name):
        """A `DOING` request was received for resource `name`"""
        return self.on_unhandled(b'DOING')

    async def on_done(self, name):
        """A `DONE` request was received for resource `name`"""
        return self.on_unhandled(b'DONE')

    async def on_exit(self):
        """An `EXIT` message was received"""
        return self.on_unhandled(b'EXIT')

    async def on_illegal(self):
        """An `ILLEGAL` message was received"""
        return self.on_unhandled(b'ILLEGAL')

    def on_invalid(self, reason):
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
    async def get(self, task):
        """Send get for task with given name"""
        msg = [b'GET', task]
        await self.send_message(msg)

    async def put(self, name, serial):
        await self.send_message([b'PUT', name, serial])

    async def not_found(self, name):
        await self.send_message([b'NOTFOUND', name])

    async def submit(self, task):
        """Send submit for given task object.

        Hereis-tasks should not be passed at all and will trigger an error.
        Handle them in a wrapper or override.
        """

        if hasattr(task, 'hereis'):
            raise ValueError('Here-is tasks must never be passed to '
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
        await self.send_message(msg)

    async def done(self, name):
        await self.send_message([b'DONE', name])

    async def doing(self, name):
        await self.send_message([b'DOING', name])

    # Main logic methods
    # ==================
    async def on_message(self, msg):
        """Parse given message, calling callbacks as needed.

        Returns:
            Bool, whether message processing should stop. This is the preferred
            way to pass back to the socket listener the information that no
            further messages are expected. Since None evaluate to False in
            boolean context, any handler without an explicit `return False` is
            interpreted as a continuation signal.

        Note: handler for invalid incoming messages does not follow this scheme,
            as an invalid incoming message is never interpreted as a normal end of
            communication. Raise exceptions from the handler if you want to
            e.g. abort on invalid messages. On the other hand, returning a stop
            condition on a 'ILLEGAL' message works, since it is a regular
            message that signals a previous error.
        """
        self.validate(len(msg) > 0, 'Empty message')
        logging.warning('Protocol: verb %s', msg[0].decode('ascii'))
        try:
            return await self.handler(str(msg[0], 'ascii'))(msg)
        except NoHandlerError:
            self.validate(False, 'No such verb')
        return False

    async def send_message(self, msg):
        """Callback method to send a message just built.

        This is the only callback that is required to be implemented.
        """
        raise NotImplementedError("You must override the send_message method "
            "when subclassing a Protocol object.")

    # Internal message handling methods
    event = EventNamespace()

    def handler(self, event_name):
        """Shortcut to call handler methods"""
        def _handler(*args, **kwargs):
            return self.event.handler(event_name)(self, *args, **kwargs)
        return _handler

    def validate(self, condition, reason='Unknown error'):
        if not condition:
            self.on_invalid(reason)

    @event.on('EXIT')
    async def _on_exit(self, msg):
        self.validate(len(msg) == 1, 'EXIT with args')
        return await self.on_exit()

    @event.on('ILLEGAL')
    async def _on_illegal(self, msg):
        self.validate(len(msg) == 1, 'ILLEGAL with args')
        return await self.on_illegal()

    @event.on('GET')
    async def _on_get(self, msg):
        self.validate(len(msg) >= 2, 'GET without a name')
        self.validate(len(msg) <= 2, 'GET with too many names')

        name = msg[1]

        return await self.on_get(name)

    @event.on('DOING')
    async def _on_doing(self, msg):
        self.validate(len(msg) >= 2, 'DOING without a name')
        self.validate(len(msg) <= 2, 'DOING with too many names')

        name = msg[1]

        return await self.on_doing(name)

    @event.on('DONE')
    async def _on_done(self, msg):
        self.validate(len(msg) >= 2, 'DONE without a name')
        self.validate(len(msg) <= 2, 'DONE with too many names')

        name = msg[1]

        return await self.on_done(name)

    @event.on('PUT')
    async def _on_put(self, msg):
        self.validate(len(msg) == 3, 'PUT with wrong number of parts')

        name = msg[1]
        serial = msg[2]

        return await self.on_put(name, serial)

    @event.on('SUBMIT')
    async def _on_submit(self, msg):
        self.validate(len(msg) >= 3, 'SUBMIT without step or tag count') # SUBMIT step n_tags

        step_name = msg[1]
        n_tags = int.from_bytes(msg[2], 'big')

        # Collect tags
        argstack = msg[3:]
        self.validate(len(argstack) >= n_tags, 'Not as many tags as stated') # Expected number of tags
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

        return await self.on_submit(name, step_name, arg_names, kwarg_names)
