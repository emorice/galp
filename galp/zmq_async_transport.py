"""
Using ØMQ as a transport for Galp protoclols
"""

import zmq

from galp.protocol import ProtocolEndException, RoutedMessage
from galp.lower_protocol import Session
import galp.messages as gm

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
        self.protocol = stack.root

        self.endpoint = endpoint

        ctx = zmq.asyncio.Context.instance()

        self.socket = ctx.socket(socket_type)
        if bind:
            self.socket.bind(endpoint)
        else:
            self.socket.connect(endpoint)

        self.session = Session(None) # Not actually used yet

    def __del__(self):
        self.socket.close()

    async def send_message(self, msg: gm.BaseMessage | RoutedMessage) -> None:
        """
        Passes msg to the protocol to be rewritten, then sends it.

        Intended to be used by application to spontaneously send a message and
        start a new communication. Not used to generate replies/reacts to an
        incoming message.
        """
        match msg:
            # Sugar interface for common high level messages
            case gm.BaseMessage():
                zmq_msg = self.stack.write_local(msg)
            # Legacy interface, to be removed
            case RoutedMessage():
                zmq_msg = self.stack.lib_upper.write_message(msg)
            # Use with legacy route_message, to be removed as well
            case list():
                zmq_msg = msg
        # write_message is allowed to supress messages, so check for it
        if zmq_msg:
            await self.send_raw(zmq_msg)

    async def send_raw(self, msg: list[bytes]) -> None:
        """
        Send a message as-is
        """
        await self.socket.send_multipart(msg)

    async def send_messages(self, messages: list[list[bytes]]):
        """
        Wrapper of send_raw accepting None to several messages
        """
        if not messages:
            return
        for message in messages:
            await self.send_raw(message)

    async def recv_message(self) -> list[list[bytes]]:
        """
        Waits for one message, then call handlers when it arrives.

        Returns what the protocol returns, normally a list of messages of the
        type accepted by protocol.write_message.
        """
        zmq_msg = await self.socket.recv_multipart()
        return self.protocol.on_message(self.session, zmq_msg)

    async def listen_reply_loop(self) -> None:
        """Simple processing loop

        Waits for a message, call the protocol handler, then sends the replies.
        Can also block on sending replies if the underlying transport does.
        Stops when a handler raises ProtocolEndException

        Suitable for peers with one connection that do not need to transfer
        information beteen connections.
        """
        try:
            while True:
                replies = await self.recv_message()
                await self.send_messages(replies)
        except ProtocolEndException:
            pass
