"""
Generic tests for galp
"""

import sys
import subprocess
import logging
import json
import asyncio

import zmq
import zmq.asyncio
import pytest

import galp.graph
import galp.steps
import galp.client


# Fixtures
# ========

@pytest.fixture
def worker():
    """Worker fixture, starts a worker in background.

    Returns:
        (endpoint, Popen) tuple
    """
    endpoint = 'tcp://127.0.0.1:48652'

    phandle = subprocess.Popen([
        sys.executable,
        '-m', 'galp.worker',
        endpoint
        ])

    yield endpoint, phandle

    # If it's a kill test it's already done, later with remote worker we'll rely
    # on messages here
    # Note: terminate should be safe to call no matter how dead the child
    # already is
    phandle.terminate()
    phandle.wait()

@pytest.fixture
def ctx():
    """The ØMQ context"""
    ctx =  zmq.Context()
    yield ctx
    # During testing we do a lot of fancy stuff such as trying to talk to the
    # dead and aborting a lot, so don't panic if there's still stuff in the
    # pipes -> linger.
    logging.warning('Now destroying context...')
    ctx.destroy(linger=1)
    logging.warning('Done')

@pytest.fixture
def async_ctx():
    """The ØMQ context, asyncio flavor"""
    ctx =  zmq.asyncio.Context()
    logging.warning('Fixture loop is: %s', id(asyncio.get_event_loop()))
    yield ctx
    # During testing we do a lot of fancy stuff such as trying to talk to the
    # dead and aborting a lot, so don't panic if there's still stuff in the
    # pipes -> linger.
    logging.warning('Now destroying context...')
    ctx.destroy(linger=1)
    logging.warning('Done')

@pytest.fixture
def worker_socket(ctx, worker):
    """Dealer socket connected to some worker"""
    endpoint, _ = worker

    socket = ctx.socket(zmq.DEALER)
    socket.connect(endpoint)

    yield socket

    # Closing with linger since we do not know if the test has failed or left
    # pending messages for whatever reason.
    socket.close(linger=1)

@pytest.fixture
def make_async_socket(async_ctx):
    """Factory feature to create several sockets to a set of workers.

    The return factory takes an endpoint as sole argument.

    Note that your endpoint must be able to deal with several clients !"""

    sockets = []
    def _make(endpoint):
        socket = async_ctx.socket(zmq.DEALER)
        sockets.append(socket)
        socket.connect(endpoint)
        return socket

    yield _make

    while sockets:
        # Closing with linger since we do not know if the test has failed or left
        # pending messages for whatever reason.
        sockets.pop().close(linger=1)

@pytest.fixture
def async_worker_socket(make_async_socket, worker):
    """Dealer socket connected to some worker, asyncio flavor"""
    endpoint, _ = worker
    yield make_async_socket(endpoint)

@pytest.fixture(params=[b'EXIT', b'ILLEGAL'])
def fatal_order(request):
    """All messages that should make the worker quit"""
    return request.param

@pytest.fixture
def client(async_worker_socket):
    """A client connected to a worker"""
    return galp.client.Client(async_worker_socket)

other_client = client

@pytest.fixture
def client_pair(async_worker_socket):
    """A pair of clients connected to the same socket and worker.

    Obviously unsafe for concurrent use. When we introduce routing, swap this
    implementation to use two sockets.
    """
    s = async_worker_socket
    c1 = galp.client.Client(s)
    c2 = galp.client.Client(s)
    return c1, c2

# Helpers
# =======

def asserted_zmq_recv_multipart(socket):
    selectable = [socket], [], []
    assert zmq.select(*selectable, timeout=4) == selectable
    return socket.recv_multipart()

# Tests
# =====

def test_nothing():
    """Do not assert anything, just check modules are importable and the
    functionning of the harness itself"""
    pass

def test_shutdown(ctx, worker, fatal_order):
    """Manually send a exit message to a local worker and wait a bit for it to
    terminate."""

    endpoint, worker_handle = worker

    assert worker_handle.poll() is None

    socket = ctx.socket(zmq.DEALER)
    socket.connect(endpoint)
    socket.send(fatal_order)

    assert worker_handle.wait(timeout=4) == 0

    # Note: we only close on normal termination, else we rely on the fixture
    # finalization to set the linger before closing.
    socket.close()

@pytest.mark.parametrize('msg', [
    [b'RABBIT'],
    [b'GET'],
    [b'GET', b'one', b'two'],
    [b'SUBMIT'],
    [b'SUBMIT', b'step', b'keyword_without_value'],
    ])
def test_illegals(worker_socket, msg):
    """Tests a few messages that should fire back an illegal

    Note that we should pick some that will not be valid later"""

    worker_socket.send_multipart(msg)

    ans = asserted_zmq_recv_multipart(worker_socket)
    assert ans == [b'ILLEGAL']

def test_task(worker_socket):
    task = galp.steps.galp_hello()
    step_name = task.step.key

    logging.warning('Calling task %s', step_name)

    handle = galp.graph.Task.gen_name(step_name, [], {})

    worker_socket.send_multipart([b'SUBMIT', step_name])

    ans = asserted_zmq_recv_multipart(worker_socket)
    assert ans == [b'DOING', handle]

    ans = asserted_zmq_recv_multipart(worker_socket)
    assert ans == [b'DONE', handle]

    worker_socket.send_multipart([b'GET', handle])

    ans = asserted_zmq_recv_multipart(worker_socket)
    assert ans[:2] == [b'PUT', handle]
    assert json.loads(ans[2]) == 42

def test_notfound(worker_socket):
    """Tests the answer of server when asking to send unexisting resource"""
    bad_handle = b'RABBIT'
    worker_socket.send_multipart([b'GET', bad_handle])

    ans = asserted_zmq_recv_multipart(worker_socket)
    assert ans == [b'NOTFOUND', bad_handle]

def test_reference(worker_socket):
    """Tests passing the result of a task to an other through handle"""

    task1 = galp.steps.galp_double()

    task2 = galp.steps.galp_double(task1)

    worker_socket.send_multipart([b'SUBMIT', task1.step.key])
    # doing
    doing = asserted_zmq_recv_multipart(worker_socket)
    done = asserted_zmq_recv_multipart(worker_socket)
    assert doing, done == ([b'DOING', task1.name], [b'DONE', task1.name])

    worker_socket.send_multipart([b'SUBMIT', task2.step.key, b'', task1.name])

    doing = asserted_zmq_recv_multipart(worker_socket)
    done = asserted_zmq_recv_multipart(worker_socket)
    assert doing, done == ([b'DOING', task2.name], [b'DONE', task2.name])

    # Let's try async get for a twist !
    worker_socket.send_multipart([b'GET', task2.name])
    worker_socket.send_multipart([b'GET', task1.name])

    # Order of the answers is unspecified
    got_a = asserted_zmq_recv_multipart(worker_socket)
    got_b = asserted_zmq_recv_multipart(worker_socket)

    assert got_a[0] == got_b[0] == b'PUT'
    assert set((got_a[1], got_b[1])) == set((task1.name, task2.name))
    expected = {
        task1.name: 2,
        task2.name: 4
        }
    for _, name, res in [got_a, got_b]:
        assert json.loads(res) == expected[name]

@pytest.mark.asyncio
async def test_async_socket(async_worker_socket):
    sock = async_worker_socket
    await asyncio.wait_for(sock.send(b'RABBIT'), 3)
    ans = await asyncio.wait_for(sock.recv_multipart(), 3)
    assert ans == [b'ILLEGAL']

@pytest.mark.asyncio
async def test_client(client):
    """Test simple functionnalities of client"""
    task = galp.steps.galp_hello()

    ans = await asyncio.wait_for(
        client.collect(task),
        3)

    assert ans == [42,]

@pytest.mark.asyncio
async def test_task_kwargs(client):
    two = galp.steps.galp_double()
    four = galp.steps.galp_double(two)

    ref = galp.steps.galp_sub(a=four, b=two)
    same = galp.steps.galp_sub(b=two, a=four)

    opposite = galp.steps.galp_sub(a=two, b=four)

    ans = await asyncio.wait_for(
        client.collect(ref, same, opposite),
        3)

    assert tuple(ans) == (2, 2, -2)

@pytest.mark.asyncio
async def test_mem_cache(client_pair):
    """
    Test worker-side in cache memory
    """
    client1, client2 = client_pair

    task = galp.steps.galp_hello()

    ans1 = await asyncio.wait_for(client1.collect(task), 3)

    ans2 = await asyncio.wait_for(client2.collect(task), 3)

    assert ans1 == ans2 == [42]

    assert client1.submitted_count[task.name] == 1
    assert client2.submitted_count[task.name] == 1

    assert client1.run_count[task.name] == 1
    assert client2.run_count[task.name] == 0
