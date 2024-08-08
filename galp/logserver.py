"""
Utils to forward stdin/stdout between processes
"""
import os
import sys
import socket
import selectors
from contextlib import contextmanager

@contextmanager
def logserver_connect(request_id, sock_logclient):
    """
    Create new pipe and send end to log server
    """
    if sock_logclient is None:
        yield
    read_fd, write_fd = os.pipe()

    # Send read end to logserver and close it
    # Wrap the send end
    socket.send_fds(sock_logclient, [str(request_id).encode('utf8')], [read_fd])
    os.close(read_fd)

    # Write in pipe and close it
    with open(write_fd, 'w', encoding='utf8') as write_stm:
        yield write_stm

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

def logserver_register(sel: selectors.DefaultSelector, sock_logserver):
    """
    Listen for new pipes on log server

    The selector data is a callback to be called with no arguments.
    """
    def on_stream_msg(request_id, filed):
        item = os.read(filed, 4096)
        if item:
            print(f'[{request_id}]', sanitize(item), file=sys.stderr, flush=True)
        else:
            print(f'[{request_id}]', '<closed>', file=sys.stderr, flush=True)
            os.close(filed)
            sel.unregister(filed)

    def on_new_fd():
        # Receive one fd and read its content
        data, fds, _flgs, _addr = socket.recv_fds(sock_logserver, 4096, 16)
        client_id = data.decode('utf8')
        for filed in fds:
            # Make any new fd non "fork-heritable"
            os.register_at_fork(after_in_child=lambda filed=filed: os.close(filed))
            sel.register(filed, selectors.EVENT_READ,
                    lambda filed=filed: on_stream_msg(client_id, filed)
                    )

    sel.register(sock_logserver, selectors.EVENT_READ, on_new_fd)
