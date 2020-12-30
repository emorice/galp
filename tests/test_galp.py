"""
Generic tests for galp
"""

import sys
import subprocess
import logging
import zmq
import pytest
import galp
import galp.graph

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

def test_illegals(worker_socket):
    """Tests a few messages that should fire back an illegal

    Note that we should pick some that will not be valid later"""

    worker_socket.send(b'RABBIT')

    ans = asserted_zmq_recv_multipart(worker_socket)
    assert ans == [b'ILLEGAL']

def test_task(worker_socket):
    task_name = b'FOO'
    handle = galp.graph.make_handle(task_name)

    worker_socket.send_multipart([b'SUBMIT', task_name])

    ans = asserted_zmq_recv_multipart(worker_socket)
    assert ans == [b'DOING', handle]

    ans = asserted_zmq_recv_multipart(worker_socket)
    assert ans == [b'DONE', handle]
    



