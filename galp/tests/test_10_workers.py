"""
Tests direct communication with a worker, with no broker or client involved.
"""

import os
import signal
import psutil
import zmq
import msgpack # type: ignore[import]

import pytest
from async_timeout import timeout
from pydantic import TypeAdapter

import galp.tests
import galp.worker
import galp.messages as gm

# pylint: disable=redefined-outer-name
# pylint: disable=no-member

# Helpers
# =======

def make_msg(*parts):
    """
    Add dummy route to a message
    """
    return [b'', *parts]

def asserted_zmq_recv_multipart(socket):
    """
    Asserts that the socket received a message in a given time, and returns said
    message
    """
    selectable = [socket], [], []
    assert zmq.select(*selectable, timeout=4) == selectable
    return socket.recv_multipart()

def load_message(msg: list[bytes]) -> gm.Message:
    """
    Deserialize message body
    """
    assert len(msg) == 3 # null, verb, payload
    cls = gm.msr.get_type(msg[1])
    assert cls
    return TypeAdapter(cls).validate_python(
        msgpack.loads(msg[-1])
        )

def zmq_recv_message(socket) -> gm.Message:
    """
    Combined recv and deserialize message.

    Checks that only such message was sent.
    """
    return load_message(
            asserted_zmq_recv_multipart(socket)
            )

def assert_ready(socket):
    """
    Awaits a READY message on the socket
    """
    msg = zmq_recv_message(socket)
    assert isinstance(msg, gm.Ready)

# Tests
# =====

@pytest.mark.parametrize('sig', [signal.SIGINT, signal.SIGTERM])
def test_signals(worker_socket, sig):
    """Test for termination on INT and TERM)"""

    socket, _endpoint, worker_handle = worker_socket

    assert worker_handle.poll() is None
    assert_ready(socket)

    process = psutil.Process(worker_handle.pid)
    children = process.children(recursive=True)

    # If not our children list may not be valid
    assert process.status() != psutil.STATUS_ZOMBIE

    worker_handle.send_signal(sig)

    _gone, alive = psutil.wait_procs([process, *children], timeout=4)
    assert not alive

@pytest.mark.parametrize('msg_body', [
    [b'RABBIT'],
    [b'GET'],
    [b'GET', b'one', b'two'],
    [b'SUBMIT'],
    [b'SUBMIT', b'RABBIT'],
    [b'SUBMIT', b'name', b'step', b'keyword_without_value'],
    ])
def test_illegals(worker_socket, msg_body):
    """Tests a few messages that should fire back an illegal

    Note that we should pick some that will not be valid later"""

    socket, *_ = worker_socket

    # Mind the empty frame on both send and receive sides
    socket.send_multipart(make_msg(*msg_body))

    assert_ready(socket)

    msg = zmq_recv_message(socket)
    assert isinstance(msg, gm.Illegal)

async def test_fork_worker(tmpdir):
    """
    Workers can be created through a fork based call
    """
    socket = zmq.asyncio.Context.instance().socket(zmq.DEALER)
    socket.bind('tcp://127.0.0.1:*')
    endpoint = socket.getsockopt(zmq.LAST_ENDPOINT)

    pid = galp.worker.fork(dict(
        endpoint=endpoint,
        store=tmpdir
        ))
    try:
        async with timeout(3):
            msg = await socket.recv_multipart()

        assert isinstance(load_message(msg), gm.Ready)
    finally:
        os.kill(pid, signal.SIGKILL)
