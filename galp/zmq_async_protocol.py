"""
Galp over ØMQ
"""

import zmq

from galp.protocol import Protocol

class ZmqAsyncProtocol(Protocol):
    """
    Args:
        name: short string that will be used in log messages to identify this
            connection
        endpoint: zmq endpoint to connect to
        socket_type: zmq socket type
        bind: whether to bind or connect, default False (connect)
    """
    def __init__(self, name, endpoint, socket_type, bind=False, **kwargs):
        router = (socket_type == zmq.ROUTER)
        super().__init__(name, router=router, **kwargs)

        self.endpoint = endpoint

        ctx = zmq.asyncio.Context.instance()

        self.socket = ctx.socket(socket_type)
        if bind:
            self.socket.bind(endpoint)
        else:
            self.socket.connect(endpoint)

    def __del__(self):
        self.socket.close()

    async def send_message(self, msg):
        return await self.socket.send_multipart(msg)

    async def on_unhandled(self, verb):
        """
        Async wrapper around sync default, logs and return False (non-fatal)
        """
        return super().on_unhandled(verb)

    async def on_ping(self, route):
        """
        Async wrapper around sync default.
        """
        return super().on_ping(route)

    async def process_one(self):
        """
        Waits for one message, then call handlers when it arrives.

        Convention for return code: True is a hint to stop, False (including
        None, the default) is a hint to continue.
        """
        msg = await self.socket.recv_multipart()
        return await self.on_message(msg)

    async def listen(self):
        """Default processing loop

        Invokes callback on each received message until one returns True
        """
        terminate = False
        while not await self.process_one():
            pass
