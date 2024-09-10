"""
Unit tests for socket_transport module
"""
import socket
import threading
import time
import random

from galp.socket_transport import send_multipart, make_receiver, BUFSIZE

def test_large_segment():
    """
    Send and receive a large payload
    """
    frame = b''.join((
        random.randbytes(2**26) # can't generate more at once
        for _ in range(2**4)
        ))

    s1, s2 = socket.socketpair()

    def _send():
        send_multipart(s1, [frame])
        s1.close()

    sender = threading.Thread(target=_send)
    sender.start()

    recv_msgs = []

    receive = make_receiver(recv_msgs.append)

    t_start = time.time()
    while True:
        buf = s2.recv(BUFSIZE)
        if not buf:
            break
        receive(buf)
    print('Received in', time.time() - t_start, 's')

    sender.join()

    assert len(recv_msgs) == 1
    msg = recv_msgs[0]
    assert len(msg) == 1
    assert msg[0] == frame
