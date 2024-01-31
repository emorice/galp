"""
Using ØMQ as a transport for Galp protoclols
"""

import zmq

from galp.protocol import ProtocolEndException
from galp.writer import TransportMessage
from galp.net.core.types import Message
from galp.serialize import LoadError

class ZmqAsyncTransport:
    """
    Args:
        stack: Protocol stack object with callbacks to handle messages. Only the
            stack root is normally needed but we have a legacy message writing path
            that uses the top layer too
        endpoint: zmq endpoint to connect to
        socket_type: zmq socket type
        bind: whether to bind or connect, default False (connect)
    """
    def __init__(self, stack, endpoint, socket_type, bind=False):
        self.stack = stack
        self.handler = stack.handler

        self.endpoint = endpoint

        ctx = zmq.asyncio.Context.instance()

        self.socket = ctx.socket(socket_type)
        if bind:
            self.socket.bind(endpoint)
        else:
            self.socket.connect(endpoint)

    def __del__(self):
        self.socket.close()

    async def send_message(self, msg: Message) -> None:
        """
        Passes msg to the protocol to be serialized, then sends it.

        Intended to be used by application to spontaneously send a message and
        start a new communication. Not used to generate replies/reacts to an
        incoming message.
        """
        await self.send_raw(self.stack.write_local(msg))

    async def send_raw(self, msg: TransportMessage) -> None:
        """
        Send a message as-is
        """
        await self.socket.send_multipart(msg)

    async def send_messages(self, messages: list[TransportMessage | LoadError]
            ) -> LoadError | None:
        """
        Wrapper of send_raw accepting several messages or errors.

        Send messages up to the first error. Return None if all messages were
        processed, and the error if one was encountered.
        """
        for message in messages:
            if isinstance(message, LoadError):
                return message
            await self.send_raw(message)
        return None

    async def recv_message(self) -> list[TransportMessage | LoadError]:
        """
        Waits for one message, then call handlers when it arrives.

        Returns what the protocol returns, normally a list of messages of the
        type accepted by protocol.write_message.
        """
        zmq_msg = await self.socket.recv_multipart()
        ret = self.handler(lambda msg: msg, zmq_msg)
        assert ret is not None, zmq_msg
        return ret

    async def listen_reply_loop(self) -> LoadError | None:
        """Simple processing loop

        Waits for a message, call the protocol handler, then sends the replies.
        Can also block on sending replies if the underlying transport does.
        Stops when a handler raises ProtocolEndException or when a parsing error
        is encountered.

        Suitable for peers with one connection that do not need to transfer
        information beteen connections.
        """
        try:
            error = None
            while error is None:
                # Replies can contain errors, but we check this when sending
                replies = await self.recv_message()
                error = await self.send_messages(replies)
            return error
        except ProtocolEndException:
            return None
