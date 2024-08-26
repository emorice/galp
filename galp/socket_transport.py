"""
Framing utils to transfer multipart messages over a classical socket

This module is getting quicky messy because we need to support four interfaces:
synchronous code, async code with selectors, async code with asyncio, and
drop-in for zmq_async_transport.  Over time we will convert and drop support for
most of these.
"""
import socket
import asyncio
import logging
from typing import Iterable

from galp.result import Result, Ok, Error
from galp.writer import TransportMessage
from galp.protocol import Stack, TransportReturn
from galp.net.core.types import Message

def send_frame(sock: socket.socket, frame: bytes, send_more: bool) -> None:
    """
    Send a single frame over a stream socket

    The protocol is very simple, we send the size first as a four byte integer,
    shifted one bit, and use that first bit to store the `send_more` flag. This
    caps the frame size at 2 GiB.
    """
    size = len(frame)
    try:
        header = ((size << 1) | int(send_more)).to_bytes(4, 'little')
    except OverflowError as exc:
        raise ValueError('Frame too long for this transport') from exc
    sock.sendall(header)
    sock.sendall(frame)

def send_multipart(sock: socket.socket, message: TransportMessage) -> None:
    """
    Send a multipart message over a stream socket
    """
    for frame in message[:-1]:
        send_frame(sock, frame, send_more=True)
    if message:
        send_frame(sock, message[-1], send_more=False)

def on_bytes(fixed):
    """
    Generator yielding hints at how much data to read, accepting any number of bytes.
    Forwards full frames to `fixed`, which yields the exact wanted size, and
    accepts the corresponding full frame.
    """
    buf = b''
    size = fixed.send(None)
    while True:
        while len(buf) < size:
            buf += yield size - len(buf)
        frame, buf = buf[:size], buf[size:]
        size = fixed.send(frame)

def on_fixed(framed):
    """Generator yielding number of exact bytes to read, accepting bytes
    Forwards dynamically-sized frames and send-more bits to `framed`, which
    accepts nothing
    """
    framed.send(None)
    while True:
        buf = yield 4
        header = int.from_bytes(buf, 'little')
        size = header >> 1
        send_more = bool(header & 1)
        frame = yield size
        framed.send((frame, send_more))

def on_frame(on_multipart):
    """Generator yielding nothing, accepting frames and send-more flags.
    Calls callback with each multipart message
    """
    while True:
        message = []
        send_more = True
        while send_more:
            frame, send_more = yield
            message.append(frame)
        on_multipart(message)

def _on_multipart(message):
    raise Done(message)

def make_multipart_generator(on_multipart=_on_multipart):
    """
    Generator accepting yielding read length cues, accepting bytes, calling
    on_multipart with each multipart message
    """
    return on_bytes(
            on_fixed(
                on_frame(
                    on_multipart
                    )
                )
            )

class Done(BaseException):
    """
    Glue exception.

    Works exactly like StopIteration, but is meant to voluntarily bubble up
    through nested generators
    """
    def __init__(self, value):
        self.value = value

def recv_multipart(sock: socket.socket) -> TransportMessage:
    """
    Receives a multipart message
    """
    _on_bytes = make_multipart_generator()
    size = _on_bytes.send(None)
    while True:
        try:
            size = _on_bytes.send(sock.recv(size))
        except Done as done:
            return done.value

async def async_recv_multipart(sock: socket.socket, loop=None) -> TransportMessage:
    """
    Receives a multipart message, async flavor
    """
    if loop is None:
        loop = asyncio.get_event_loop()
    _on_bytes = make_multipart_generator()
    size = _on_bytes.send(None)
    while True:
        try:
            if size > 0:
                buf = await loop.sock_recv(sock, size)
                if not buf:
                    raise EOFError
            else:
                buf = b''
            size = _on_bytes.send(buf)
        except Done as done:
            return done.value

class WriterAdapter:
    """
    Adapter to use classical socket code with asyncio writer
    """
    def __init__(self, writer: asyncio.StreamWriter):
        self.writer = writer

    def sendall(self, frame: bytes):
        """
        Wraps write
        """
        self.writer.write(frame)

    async def drain(self):
        """
        Wraps drain
        """
        await self.writer.drain()

class ReaderAdapter: # pylint: disable=too-few-public-methods
    """
    Adapter to use classical socket code with asyncio reader
    """
    @classmethod
    async def sock_recv(cls, sock, size: int):
        """
        Wraps read
        """
        return await sock.read(size)

class AsyncTransport:
    """
    Args:
        stack: Protocol stack object with callbacks to handle messages. Only the
            stack root is normally needed but we have a legacy message writing path
            that uses the top layer too
        endpoint: endpoint to connect to, using zmq syntax (ipc://... or tcp://...)
        bind: whether to bind (and expose connection ids), or connect
    """
    def __init__(self, stack: Stack, endpoint: str, bind=False):
        self.stack: Stack = stack
        self.handler = stack.handler

        self.endpoint: str = endpoint
        self.bind = bind
        self.writers: dict[bytes, WriterAdapter] = {}
        self.next_client_id = 0
        self.queue: asyncio.Queue[TransportMessage] = asyncio.Queue()
        self.reader: asyncio.StreamReader
        self._ready = asyncio.Event()
        self._starting = False

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
        await self.ensure_setup()
        if self.bind:
            cid, *msg = msg
            try:
                writer_adapter = self.writers[cid]
            except KeyError:
                logging.error('Dropping message to client %s', cid)
        else:
            writer_adapter = self.writers[b'']

        send_multipart(writer_adapter, msg) # type: ignore[arg-type] # adapter
        await writer_adapter.drain()

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
        if self.bind:
            msg = await self.queue.get()
        else:
            msg = await async_recv_multipart(self.reader, ReaderAdapter) # type: ignore[arg-type]
        return self.handler(lambda msg: msg, msg)

    async def accept(self, reader, writer):
        """
        On connect, register a writer for peer, and start waiting on reader to
        queue messages received
        """
        logging.info('Accepting')
        cid = self.next_client_id.to_bytes(4, 'little')
        self.next_client_id += 1

        self.writers[cid] = WriterAdapter(writer)

        try:
            while True:
                msg = await async_recv_multipart(reader, ReaderAdapter)
                await self.queue.put([cid, *msg])
        except EOFError:
            del self.writers[cid]


    async def ensure_setup(self):
        """
        Wait for setup
        """
        # Short path: we're already online
        if self._ready.is_set():
            return

        # If not, setup
        if self._starting:
            # A concurrent call is doing the setup; wait
            await self._ready.wait()
        else:
            # We're first, do the setup
            self._starting = True
            await self.setup()
            self._ready.set()

    async def setup(self):
        """
        Call try_setup in loopif needed
        """
        delay = .1
        while True:
            try:
                await self.try_setup()
                break
            except ConnectionRefusedError:
                logging.info('Failed setup, retrying in %s', delay)
                await asyncio.sleep(delay)
                delay *= 2
        logging.info('Ready !')

    async def try_setup(self):
        """
        Bind or connect
        """
        if self.endpoint.startswith('tcp://'):
            host, s_port = self.endpoint[6:].split(':')
            port = int(s_port)
            if self.bind:
                await asyncio.start_server(self.accept, host, port)
            else:
                self.reader, writer = await asyncio.open_connection(host, port)
                self.writers[b''] = WriterAdapter(writer)
        elif self.endpoint.startswith('ipc://'):
            path = self.endpoint[6:]
            if path[0] == '@':
                path = '\x00' + path[1:]
            if self.bind:
                logging.info('Listening %s', path)
                await asyncio.start_unix_server(self.accept, path)
            else:
                logging.info('Connecting %s', path)
                self.reader, writer = await asyncio.open_unix_connection(path)
                self.writers[b''] = WriterAdapter(writer)
        else:
            raise ValueError(f'Bad endpoint: {self.endpoint}')

    async def listen_reply_loop(self) -> Result[object]:
        """Message processing loop

        Waits for a message, call the protocol handler, then sends the replies.
        Can also block on sending replies if the underlying transport does.
        Stops when a handler returns a Result.
        """
        await self.ensure_setup()

        while True:
            replies = await self.recv_message()
            if isinstance(replies, Error):
                return replies
            if isinstance(replies, Ok):
                return replies
            await self.send_messages(replies)
