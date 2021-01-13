"""
GALP protocol implementation
"""

import json
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
    async def on_get(self, name):
        """A `GET` request was received for resource `name`"""
        self.on_unhandled(b'GET')

    async def on_put(self, name, obj):
        """A `PUT` message was received for resource `name` with object `obj`"""
        self.on_unhandled(b'PUT')

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


    # Send methods
    # ============
    async def get(self, task):
        """Send get for task with given name"""
        msg = [b'GET', task]
        await self.send_message(msg)

    async def put(self, name, obj):
        payload = json.dumps(obj).encode('ascii')
        await self.send_message([b'PUT', name, payload])

    async def not_found(self, name):
        await self.send_message([b'NOTFOUND', name])

    # Main logic methods
    # ==================
    async def on_message(self, msg):
        """Parse given message, calling callbacks as needed."""
        logging.warning('Protocol: verb %s', msg[0].decode('ascii'))
        try:
            await self.handler(str(msg[0], 'ascii'))(msg)
        except NoHandlerError:
            self.validate(False, 'No such verb')

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

    @event.on('GET')
    async def _on_get(self, msg):
        self.validate(len(msg) >= 2, 'GET without a name')
        self.validate(len(msg) <= 2, 'GET with too many names')

        name = msg[1]

        await self.on_get(name)

    @event.on('PUT')
    async def _on_put(self, msg):
        self.validate(len(msg) == 3, 'PUT with wrong number of parts')

        name = msg[1]
        obj = json.loads(msg[2])

        await self.on_put(name, obj)



    @event.on('SUBMIT')
    async def _on_submit(self, msg):
        self.validate(len(msg) >= 3, 'SUBMIT without step of tag count') # SUBMIT step n_tags

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
                keyword, arg_handle = argstack.pop(), argstack.pop()
            except IndexError:
                raise IllegalRequestError
            if keyword == b'':
                arg_names.append(arg_handle)
            else:
                kwarg_names[keyword] = arg_handle

        handle = galp.graph.Task.gen_name(step_name, arg_names, kwarg_names, vtags)

        await self.on_submit(handle, step_name, arg_names, kwarg_names)

