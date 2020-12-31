"""
Generic tests for galp
"""

import sys
import subprocess
import logging
import zmq
import pytest
import json
import galp
import galp.graph
import galp.steps

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
    ctx =  zmq.Context.instance()
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

def test_nothing():
    """Do not assert anything, just check modules are importable and the
    functionning of the harness itself"""
    pass

@pytest.fixture(params=[b'EXIT', b'ILLEGAL'])
def fatal_order(request):
    """All messages that should make the worker quit"""
    return request.param

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

def asserted_zmq_recv_multipart(socket):
    selectable = [socket], [], []
    assert zmq.select(*selectable, timeout=4) == selectable
    return socket.recv_multipart()

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



