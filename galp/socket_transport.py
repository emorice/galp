"""
Framing utils to transfer multipart messages over a classical socket
"""
import socket
import asyncio

from galp.writer import TransportMessage

def send_frame(sock: socket.socket, frame: bytes, send_more: bool):
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

def send_multipart(sock: socket.socket, message: TransportMessage):
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

def make_multipart_protocol(on_multipart):
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
    def _on_multipart(message):
        raise Done(message)
    _on_bytes = make_multipart_protocol(_on_multipart)
    size = _on_bytes.send(None)
    while True:
        try:
            size = _on_bytes.send(sock.recv(size))
        except Done as done:
            return done.value

# Asyncio copy-paste version, because every other solution is going to be much
# more complicated than copy-pasting three functions
## s/def /async def async_/
## s/sock.recv(/await asyncio.get_event_loop().sock_recv(sock, /
## s/ recv/await async_recv

async def async_recv_exact(sock: socket.socket, size: int) -> bytes:
    """
    Naive fixed size reader
    """
    buf = b''
    while len(buf) < size:
        buf += await asyncio.get_event_loop().sock_recv(sock, size - len(buf))
    return buf

async def async_recv_frame(sock: socket.socket) -> tuple[bytes, bool]:
    """
    Receives a single frame and send_more bit
    """

    header = int.from_bytes(await async_recv_exact(sock, 4), 'little')
    size = header >> 1
    send_more = bool(header & 1)
    frame =await async_recv_exact(sock, size)
    return frame, send_more

async def async_recv_multipart(sock: socket.socket) -> TransportMessage:
    """
    Receives a multipart message
    """
    message = []
    send_more = True
    while send_more:
        frame, send_more =await async_recv_frame(sock)
        message.append(frame)
    return message
