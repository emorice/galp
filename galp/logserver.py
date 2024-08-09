"""
Utils to forward stdin/stdout between processes
"""
import os
import sys
import socket
import selectors
from contextlib import contextmanager
from typing import BinaryIO

import galp.socket_transport
from galp.net.core.types import RequestId, Reply, Progress
from galp.net.core.dump import dump_message

@contextmanager
def logserver_connect(request_id: RequestId, sock_logclient: socket.socket |
        None):
    """
    Create new pipe, redirect stderr and stdout to it, and send end to log server
    """
    if sock_logclient is None:
        yield
        return
    read_fd_out, write_fd_out = os.pipe()
    read_fd_err, write_fd_err = os.pipe()

    # Send read end to logserver and close it
    # Wrap the send end
    socket.send_fds(sock_logclient, [request_id.as_word()],
            [read_fd_out, read_fd_err])
    os.close(read_fd_out)
    os.close(read_fd_err)

    # Redirect to write fd
    sys.stdout.flush()
    sys.stderr.flush()
    orig_stds = os.dup(1), os.dup(2)
    os.dup2(write_fd_out, 1)
    os.dup2(write_fd_err, 2)
    try:
        yield
    finally:
        sys.stdout.flush()
        sys.stderr.flush()
        os.dup2(orig_stds[0], 1)
        os.dup2(orig_stds[1], 2)

def sanitize(buffer: bytes) -> str:
    """
    Output may be anything. To not mangle our display, try to coerce it into
    something we can handle. On failure, just return an ugly escaped version.
    """
    # Try to decode as utf8
    try:
        string = buffer.decode('utf8')
    except UnicodeDecodeError:
        return str(buffer)

    # Then, emulate or ignore common non-printable characters:

    # 1. Strip exactly one final `\n`
    stripped = string[:-1] if string[-1] == '\n' else string

    # 2. Keep only the last line
    after_n = stripped.rpartition('\n')[-1]

    # 3. Attempt to emulate '\r' by keeping only what's after
    after_r = after_n.rpartition('\r')[-1]

    # 4. Expand tabs
    expanded = after_r.expandtabs()

    # If any non-printable character remains, print an escaped version
    if expanded.isprintable():
        printable = expanded
    else:
        printable = repr(expanded)

    # Truncate to 80 chars
    return printable[:80]

def logserver_register(sel: selectors.DefaultSelector, sock_logserver:
        socket.socket, sock_proxy: socket.socket, log_dir: str):
    """
    Listen for new pipes on log server

    The selector data is a callback to be called with no arguments.
    Reply/Progress messages are send on sock_proxy.
    All data is tee'd to files in log_dir.
    """
    def on_stream_msg(request_id: RequestId, filed: int,
            tee_file: BinaryIO | None):
        item = os.read(filed, 4096)
        if item:
            if tee_file:
                tee_file.write(item)
            try:
                item_s = item.decode('utf8')
            except UnicodeDecodeError:
                item_s = f'<failed utf-8 decoding of {len(item)} bytes>'
            galp.socket_transport.send_multipart(
                    sock_proxy,
                    dump_message(Reply(request_id, Progress(item_s)))
                    )
        else:
            if tee_file:
                tee_file.close()
            os.close(filed)
            sel.unregister(filed)

    def on_new_fd():
        # Receive one fd and read its content
        data, fds, _flgs, _addr = socket.recv_fds(sock_logserver, 4096, 16)
        request_id = RequestId.from_word(data)
        for i, filed in enumerate(fds):
            tee_file = None
            if request_id.verb == b'submit':
                ext = 'out' if i == 0 else ('err' if i == 1 else str(i))
                tee_file = open( #pylint: disable=consider-using-with # Async
                        os.path.join(log_dir, f'{request_id.name.hex()}.{ext}'),
                        'wb', buffering=0)
            # Make any new fd non "fork-heritable"
            os.register_at_fork(after_in_child=lambda filed=filed: os.close(filed))
            sel.register(filed, selectors.EVENT_READ,
                    lambda filed=filed, tee_file=tee_file: on_stream_msg(
                        request_id, filed, tee_file
                        )
                    )

    sel.register(sock_logserver, selectors.EVENT_READ, on_new_fd)
