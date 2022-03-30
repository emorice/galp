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
    def __init__(self, name, endpoint, socket_type, bind=False):
        super().__init__(name)

        self.endpoint = endpoint

        ctx = zmq.asyncio.Context()

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
