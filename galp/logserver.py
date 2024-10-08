"""
Utils to forward stdin/stdout between processes
"""
import os
import sys
import socket
import selectors
import logging
from contextlib import contextmanager
from typing import BinaryIO

import galp.socket_transport
from galp.net.core.types import RequestId, Reply, Progress, TaskProgress
from galp.protocol import write_local

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
        # Flush, restore std fds, and close saved fds and pipes
        # We could maybe permanently save the orig fds instead
        sys.stdout.flush()
        os.dup2(orig_stds[0], 1)
        os.close(orig_stds[0])

        sys.stderr.flush()
        os.dup2(orig_stds[1], 2)
        os.close(orig_stds[1])

        os.close(write_fd_out)
        os.close(write_fd_err)

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
    # keep track of all received fds, because when we fork a new process the
    # child has to close them all
    all_fds: set[int] = set()
    def _close_all():
        for fd in all_fds:
            os.close(fd)
    os.register_at_fork(after_in_child=_close_all)

    def on_stream_msg(request_id: RequestId, filed: int, orig_filed: int,
            tee_file: BinaryIO | None):
        try:
            item = os.read(filed, 4096)
        except OSError:
            logging.exception('Failed to read from worker pipe, task [%s]',
                request_id.name)
            return
        if item:
            if tee_file:
                try:
                    tee_file.write(item)
                except OSError:
                    logging.exception('Failed to write to log file, task [%s]',
                        request_id.name)
            status: TaskProgress = {
                    'event': 'stdout' if orig_filed == 1 else 'stderr',
                    'payload': item
                    }
            galp.socket_transport.send_multipart(
                    sock_proxy,
                    write_local(Reply(request_id, Progress(status)))
                    )
        else:
            # Other end finished the task or died. Close the log file, our end
            # of the pipe, and remove it from the list that children have to
            # close
            if tee_file:
                tee_file.close()
            os.close(filed)
            all_fds.remove(filed)
            sel.unregister(filed)

    def on_new_fd() -> None:
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
            all_fds.add(filed)
            sel.register(filed, selectors.EVENT_READ,
                    lambda filed=filed, tee_file=tee_file, i=i: on_stream_msg(
                        request_id, filed, i+1, tee_file,
                        )
                    )

        status: TaskProgress = {
                'event': 'started',
                'payload': b''
                }
        galp.socket_transport.send_multipart(
                sock_proxy,
                write_local(Reply(request_id, Progress(status)))
                )

    sel.register(sock_logserver, selectors.EVENT_READ, on_new_fd)
