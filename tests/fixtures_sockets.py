"""
Fixtures related to manually managing sockets connected to galp processes.
"""
import logging

import pytest

import zmq

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
    yield ctx
    # During testing we do a lot of fancy stuff such as trying to talk to the
    # dead and aborting a lot, so don't panic if there's still stuff in the
    # pipes -> linger.
    logging.warning('Now destroying context...')
    ctx.destroy(linger=1)
    logging.warning('Done')

@pytest.fixture
def worker_socket(ctx, make_worker):
    """Dealer socket connected to a standalone worker (no pool nor broker)

    Returns:
        (socket, endpoint, worker_process)
    """

    socket = ctx.socket(zmq.DEALER)
    socket.bind('tcp://127.0.0.1:*')
    endpoint = socket.getsockopt(zmq.LAST_ENDPOINT)

    endpoint, process = make_worker(endpoint)

    yield socket, endpoint, process

    # Closing with linger since we do not know if the test has failed or left
    # pending messages for whatever reason.
    socket.close(linger=1)

@pytest.fixture
def async_worker_socket(async_ctx, make_worker):
    """Dealer socket connected to a standalone worker (no pool nor broker),
    asyncio flavor

    Returns:
        (socket, endpoint, worker_handle)
    """
    socket = async_ctx.socket(zmq.DEALER)
    socket.bind('tcp://127.0.0.1:*')
    endpoint = socket.getsockopt(zmq.LAST_ENDPOINT)

    endpoint, process = make_worker(endpoint)

    yield socket, endpoint, process

    # Closing with linger since we do not know if the test has failed or left
    # pending messages for whatever reason.
    socket.close(linger=1)
