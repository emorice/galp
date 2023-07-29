"""
Tests direct communication with a worker, with no broker or client involved.
"""

import os
import asyncio
import signal
import psutil
import zmq
import msgpack

import pytest
from async_timeout import timeout
from pydantic import TypeAdapter

import galp.tests
import galp.worker
from galp.messages import Message

# pylint: disable=redefined-outer-name
# pylint: disable=no-member

# Custom fixtures
# ===============
@pytest.fixture(params=[
    [b'EXIT'],
    [b'ILLEGAL', b'You didnt say please']
    ])
def fatal_order(request):
    """All messages that should make the worker quit"""
    return request.param

# Helpers
# =======

def make_msg(*parts):
    """
    Add dummy route and counters to a message
    """
    return [b'', b'\0\0\0\0', b'\x10\0\0\0', *parts]

def is_body(msg, body):
    """
    Remove route and counter before comparing a message to a body
    """
    assert len(msg) >= 3
    return msg[3:] == body

def load_message(msg: list[bytes]) -> Message:
    """
    Deserialize message body
    """
    assert len(msg) == 5
    return TypeAdapter(Message).validate_python({
        'forward': [], 'incoming': [],
        **msgpack.loads(msg[-1])
        })


def body_startswith(msg, body_start):
    """
    Remove route and counter before comparing a message to a body
    """
    assert len(msg) >= 3
    return msg[3:3+len(body_start)] == body_start

def asserted_zmq_recv_multipart(socket):
    """
    Asserts that the socket received a message in a given time, and returns said
    message
    """
    selectable = [socket], [], []
    assert zmq.select(*selectable, timeout=4) == selectable
    return socket.recv_multipart()

def assert_ready(socket):
    """
    Awaits a READY message on the socket
    """
    ans = asserted_zmq_recv_multipart(socket)
    assert len(ans) == 5
    # Third part is a worker self-id
    assert ans[0] == b''
    # 1 and 2 are counters
    assert ans[3] == b'READY'
    # 4 is a self id

# Tests
# =====

def test_shutdown(worker_socket, fatal_order):
    """Manually send a exit message to a local worker and wait a bit for it to
    terminate."""

    socket, _endpoint, worker_handle = worker_socket

    assert worker_handle.poll() is None

    # Mind the empty frame
    socket.send_multipart(make_msg(*fatal_order))

    assert worker_handle.wait(timeout=4) == 0

    # Note: we only close on normal termination, else we rely on the fixture
    # finalization to set the linger before closing.
    socket.close()

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

    ans2 = asserted_zmq_recv_multipart(socket)
    assert body_startswith(ans2, [b'ILLEGAL'])

def test_task(worker_socket):
    """
    Tests running a task by manually sending and receiving messages
    """
    socket, *_ = worker_socket

    task = galp.steps.galp_hello()
    name = task.name

    socket.send_multipart(make_msg(b'SUBMIT',
        msgpack.packb(task.task_def.model_dump())
        ))

    assert_ready(socket)

    ans = load_message(asserted_zmq_recv_multipart(socket))
    assert (ans.verb, ans.name) == ('doing', name)

    ans = asserted_zmq_recv_multipart(socket)
    assert body_startswith(ans, [b'DONE'])

    socket.send_multipart(make_msg(b'GET', name))

    ans = asserted_zmq_recv_multipart(socket)
    assert ans[3] == b'PUT'
    assert msgpack.unpackb(ans[5]) == 42

def test_notfound(worker_socket):
    """Tests the answer of server when asking to send unexisting resource"""
    worker_socket, *_ = worker_socket

    # Use a name or the rigth size to pass validation
    bad_handle = b'RABBIT' * 5 + b'xx'
    worker_socket.send_multipart(make_msg(b'GET', bad_handle))

    assert_ready(worker_socket)

    ans = asserted_zmq_recv_multipart(worker_socket)
    assert is_body(ans, [b'NOTFOUND', bad_handle])

def test_reference(worker_socket):
    """Tests passing the result of a task to an other through handle"""
    # pylint: disable=no-member
    worker_socket, *_ = worker_socket

    task1 = galp.steps.galp_double()

    task2 = galp.steps.galp_double(task1)

    worker_socket.send_multipart(make_msg(b'SUBMIT',
        msgpack.packb(task1.task_def.model_dump())
        ))

    assert_ready(worker_socket)

    # doing
    doing = load_message(asserted_zmq_recv_multipart(worker_socket))
    done = asserted_zmq_recv_multipart(worker_socket)
    assert (doing.verb, doing.name) == ('doing', task1.name)
    assert body_startswith(done, [b'DONE'])

    worker_socket.send_multipart(make_msg(b'SUBMIT',
        msgpack.packb(task2.task_def.model_dump())
        ))

    doing = load_message(asserted_zmq_recv_multipart(worker_socket))
    done = asserted_zmq_recv_multipart(worker_socket)
    assert (doing.verb, doing.name) == ('doing', task2.name)
    assert body_startswith(done, [b'DONE'])

    # Let's try async get for a twist !
    worker_socket.send_multipart(make_msg(b'GET', task2.name))
    worker_socket.send_multipart(make_msg(b'GET', task1.name))

    # Order of the answers is unspecified
    got_a = asserted_zmq_recv_multipart(worker_socket)
    got_b = asserted_zmq_recv_multipart(worker_socket)

    assert got_a[0] == got_b[0] == b''
    assert got_a[3] == got_b[3] == b'PUT'
    expected = {2, 4}
    assert { msgpack.unpackb(got[5]) for got in (got_a, got_b) } == expected

async def test_async_socket(async_worker_socket):
    """
    Tests simple message exchange with the asyncio flavor of pyzmq
    """
    sock, _, _ = async_worker_socket

    await asyncio.wait_for(sock.send_multipart(make_msg(b'RABBIT')), 3)

    ready = await asyncio.wait_for(sock.recv_multipart(), 3)
    assert ready[3] == b'READY'

    ans = await asyncio.wait_for(sock.recv_multipart(), 3)
    assert body_startswith(ans, [b'ILLEGAL'])

async def test_fork_worker(tmpdir):
    """
    Workers can be created through a fork based call
    """
    socket = zmq.asyncio.Context.instance().socket(zmq.ROUTER)
    socket.bind('tcp://127.0.0.1:*')
    endpoint = socket.getsockopt(zmq.LAST_ENDPOINT)

    pid = galp.worker.fork(dict(
        endpoint=endpoint,
        store=tmpdir
        ))
    try:
        async with timeout(3):
            msg = await socket.recv_multipart()

        assert b'READY' in msg
    finally:
        os.kill(pid, signal.SIGKILL)
