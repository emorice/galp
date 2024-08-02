"""
Framing utils to transfer multipart messages over a classical socket
"""
import socket

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

def recv_exact(sock: socket.socket, size: int) -> bytes:
    """
    Naive fixed size reader
    """
    buf = b''
    while len(buf) < size:
        buf += sock.recv(size - len(buf))
    return buf

def recv_frame(sock: socket.socket) -> tuple[bytes, bool]:
    """
    Receives a single frame and send_more bit
    """

    header = int.from_bytes(recv_exact(sock, 4), 'little')
    size = header >> 1
    send_more = bool(header & 1)
    frame = recv_exact(sock, size)
    return frame, send_more

def recv_multipart(sock: socket.socket) -> TransportMessage:
    """
    Receives a multipart message
    """
    message = []
    send_more = True
    while send_more:
        frame, send_more = recv_frame(sock)
        message.append(frame)
    return message
