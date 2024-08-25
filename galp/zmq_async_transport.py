"""
Using ØMQ as a transport for Galp protoclols
"""
from typing import Iterable
import zmq
import zmq.asyncio

from galp.protocol import Stack, TransportReturn
from galp.writer import TransportMessage
from galp.net.core.types import Message
from galp.result import Result, Ok, Error

class ZmqAsyncTransport:
    """
    Args:
        stack: Protocol stack object with callbacks to handle messages. Only the
            stack root is normally needed but we have a legacy message writing path
            that uses the top layer too
        endpoint: zmq endpoint to connect to
        bind: whether to bind a ROUTER socket, or connect a DEALER socket.
            Default False (connect, DEALER)
    """
    def __init__(self, stack: Stack, endpoint, bind=False):
        self.stack: Stack = stack
        self.handler = stack.handler

        self.endpoint = endpoint

        ctx = zmq.asyncio.Context.instance()

        if bind:
            self.socket = ctx.socket(zmq.ROUTER)
            self.socket.bind(endpoint)
        else:
            self.socket = ctx.socket(zmq.DEALER)
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

    async def send_messages(self, messages: Iterable[TransportMessage]) -> None:
        """
        Wrapper of send_raw accepting several messages or errors.

        Send messages up to the first error. Return None if all messages were
        processed, and the error if one was encountered.
        """
        for message in messages:
            await self.send_raw(message)

    async def recv_message(self) -> TransportReturn:
        """
        Waits for one message, then call handlers when it arrives.

        Returns what the protocol returns, normally a list of messages of the
        type accepted by protocol.write_message.
        """
        zmq_msg = await self.socket.recv_multipart()
        return self.handler(lambda msg: msg, zmq_msg)

    async def listen_reply_loop(self) -> Result[object]:
        """Message processing loop

        Waits for a message, call the protocol handler, then sends the replies.
        Can also block on sending replies if the underlying transport does.
        Stops when a handler returns a Result.
        """
        while True:
            replies = await self.recv_message()
            if isinstance(replies, Error):
                return replies
            if isinstance(replies, Ok):
                return replies
            await self.send_messages(replies)
