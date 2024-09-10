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
import time
from typing import Iterable
from dataclasses import dataclass

from galp.result import Result, Ok, Error
from galp.protocol import TransportHandler, write_local, TransportMessage
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

def make_receiver(callback):
    """
    Build a message parser
    """
    buffers = [] # Leftover bytes from previous calls
    size = 0 # Total number of bytes in old and new buffers
    next_size = 4 # Size of next expected segment
    # 3-state for position in message:
    #  None = at the beginning of new frame
    #  True = after header, more frames to follow
    #  False = after header, last frame
    send_more = None
    message = [] # Actual list of frames

    def _on_bytes(new_buf: bytes) -> None:
        nonlocal buffers
        nonlocal size
        nonlocal next_size
        nonlocal send_more
        nonlocal message

        # Concatenate to any leftovers from previous calls
        buffers.append(new_buf)
        size += len(new_buf)

        # Parse as many segments as possible
        while size >= next_size:
            # Concatenate all pending buffers
            buf = b''.join(buffers)
            segment, buf = buf[:next_size], buf[next_size:]
            # Save leftovers
            buffers = [buf]
            size = len(buf)

            if send_more is None:
                # Parse a header segment
                header = int.from_bytes(segment, 'little')
                next_size = header >> 1
                send_more = bool(header & 1)
            else:
                # Add a frame
                message.append(segment)
                # Maybe emit full message
                if not send_more:
                    callback(message)
                    message = []
                # Reset
                send_more = None
                next_size = 4

    return _on_bytes

def connect(endpoint: str) -> socket.socket:
    """
    Synchronously connect to endpoint
    """

    # Parse endpoint
    if endpoint.startswith('tcp://'):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        host, port = endpoint[6:].split(':')
        address: str | tuple[str, int] = host, int(port)
    elif endpoint.startswith('ipc://'):
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        path = endpoint[6:]
        if path[0] == '@':
            path = '\x00' + path[1:]
        address = path
    else:
        raise ValueError(f'Bad endpoint: {endpoint}')
    logging.info('Connecting %s', address)

    delay = .1
    while delay < 30.:
        try:
            sock.connect(address)
            logging.info('Connected to %s', address)
            return sock
        except ConnectionRefusedError:
            logging.info('Failed setup, retrying in %s', delay)
            time.sleep(delay)
            delay *= 2

    raise TimeoutError(f'Could not connect to {address}')

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

BUFSIZE = 4096 * 16

class AsyncClientTransport:
    """
    Args:
        stack: Protocol stack object with callbacks to handle messages. Only the
            stack root is normally needed but we have a legacy message writing path
            that uses the top layer too
        endpoint: endpoint to connect to, using zmq syntax (ipc://... or tcp://...)
        bind: whether to bind (and expose connection ids), or connect
    """
    def __init__(self, handler: TransportHandler, endpoint: str):
        self.handler = handler

        self.endpoint: str = endpoint
        self.writer: WriterAdapter
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
        await self.send_messages([write_local(msg)])

    async def send_messages(self, messages: Iterable[TransportMessage]) -> None:
        """
        Wrapper of send_raw accepting several messages or errors.

        Send messages up to the first error. Return None if all messages were
        processed, and the error if one was encountered.
        """
        await self.ensure_setup()
        for message in messages:
            send_multipart(self.writer, message) # type: ignore[arg-type] # adapter
        await self.writer.drain()

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
            host, port = self.endpoint[6:].split(':')
            self.reader, writer = await asyncio.open_connection(host, int(port))
        elif self.endpoint.startswith('ipc://'):
            path = self.endpoint[6:]
            if path[0] == '@':
                path = '\x00' + path[1:]
            logging.info('Connecting %s', path)
            self.reader, writer = await asyncio.open_unix_connection(path)
        else:
            raise ValueError(f'Bad endpoint: {self.endpoint}')
        self.writer = WriterAdapter(writer)

    async def listen_reply_loop(self) -> Result[object]:
        """Message processing loop

        Waits for a message, call the protocol handler, then sends the replies.
        Can also block on sending replies if the underlying transport does.
        Stops when a handler returns a Result.
        """
        await self.ensure_setup()

        # Cumulative replies
        final = None

        # Set final or send replies on each message
        def _cb(msg):
            nonlocal final
            replies = self.handler(msg)
            if isinstance(replies, Error):
                final = replies
            elif isinstance(replies, Ok):
                final = replies
            else:
                for rep in replies:
                    # type: ignore[arg-type] # adapter
                    send_multipart(self.writer, rep)

        # Make parser
        receive = make_receiver(_cb)

        # Loop
        while not final:
            buf = await self.reader.read(BUFSIZE)
            if not buf:
                raise EOFError
            receive(buf)
            await self.writer.drain()
        return final

@dataclass
class _Clients:
    writers: dict[bytes, WriterAdapter]
    queue: asyncio.Queue[TransportMessage]
    next_client_id: int = 0

async def _accept(clients: _Clients, reader, writer):
    """
    On connect, register a writer for peer, and start waiting on reader to
    queue messages received
    """
    logging.info('Accepting')
    cid = clients.next_client_id.to_bytes(4, 'little')
    clients.next_client_id += 1

    clients.writers[cid] = WriterAdapter(writer)
    receive = make_receiver(lambda msg: clients.queue.put_nowait([cid, *msg]))

    while True:
        buf = await reader.read(BUFSIZE)
        if not buf:
            break
        receive(buf)
    del clients.writers[cid]

async def _async_bind(endpoint: str, accept):
    """
    Bind
    """
    if endpoint.startswith('tcp://'):
        host, port = endpoint[6:].split(':')
        return await asyncio.start_server(
                accept, host, int(port)
                )
    if endpoint.startswith('ipc://'):
        path = endpoint[6:]
        if path[0] == '@':
            path = '\x00' + path[1:]
        logging.info('Listening %s', path)
        return  await asyncio.start_unix_server(accept, path)
    raise ValueError(f'Bad endpoint: {endpoint}')

async def serve(endpoint: str, handler: TransportHandler) -> Result[object]:
    """Message processing loop

    Waits for a message, call the protocol handler, then sends the replies.
    Can also block on sending replies if the underlying transport does.
    Stops when a handler returns a Result.
    """
    writers: dict[bytes, WriterAdapter] = {}
    queue: asyncio.Queue[TransportMessage] = asyncio.Queue()
    clients = _Clients(writers, queue)
    server = None

    try:
        server = await _async_bind(endpoint, lambda rdr, wtr: _accept(clients, rdr, wtr))

        while True:
            msg = await clients.queue.get()
            replies = handler(msg)
            if isinstance(replies, Error):
                break
            if isinstance(replies, Ok):
                break
            for msg in replies:
                cid, *msg = msg
                try:
                    writer_adapter = clients.writers[cid]
                except KeyError:
                    logging.error('Dropping message to client %s', cid)

                send_multipart(writer_adapter, msg) # type: ignore[arg-type] # adapter
                await writer_adapter.drain()
    finally:
        if server is not None:
            server.close()
            # fixme: this should be the proper cleanup but makes us hang
            #await server.wait_closed()

    return replies
