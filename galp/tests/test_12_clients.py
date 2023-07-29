"""
Tests of client behavior, not connected to a full broker or worker
"""

import asyncio
import logging
import zmq

import pytest
from async_timeout import timeout

import galp
import galp.tests.steps as gts
from galp.protocol import Protocol
from galp.zmq_async_transport import ZmqAsyncTransport
from galp.messages import Doing

# pylint: disable=redefined-outer-name

@pytest.fixture
async def peer_client():
    """
    A client and connected peer
    """
    # Inproc seems to have the most consistent buffering behavior,
    # understandably
    endpoint = 'inproc://test_fill_queue'
    peer = ZmqAsyncTransport(
        Protocol('CL', router=False),
        endpoint, zmq.DEALER, bind=True) # pylint: disable=no-member

    client = galp.Client(endpoint)

    yield peer, client

    peer.socket.close(linger=1)
    client.transport.socket.close(linger=1)

@pytest.fixture
async def blocked_client(peer_client):
    """
    A Client and bound socket with the client sending queue artificially filled
    """
    peer, client = peer_client

    # Lower the HWMs first, else filling the queues takes lots of messages
    # 0 creates problems, 1 seems the minimum still safe
    client.transport.socket.hwm = 1
    peer.socket.hwm = 1

    fillers = [0]
    async def _fill():
        # pylint: disable=no-member
        await client.transport.socket.send(b'FILLER', flags=zmq.NOBLOCK)
        fillers[0] += 1

    with pytest.raises(zmq.ZMQError):
        # Send messages until we error or timeout to fill the queue
        # If this work zmq should eventually raise
        async with timeout(1):
            while True:
                await _fill()
    logging.warning('Queue filled after %d fillers', fillers[0])

    # Check that we can reliably not send anything more
    for _ in range(100):
        with pytest.raises(zmq.ZMQError):
            await _fill()

    yield peer, client

async def test_fill_queue(blocked_client):
    """
    Tests that we can saturate the client outgoing queue at will
    """
    _, client = blocked_client
    task = gts.hello()
    route = client.protocol.default_route()

    # Check that client blocks
    with pytest.raises(asyncio.TimeoutError):
        async with timeout(1):
            await client.transport.send_message(
                client.protocol.submit(route, task.task_def)
                )

async def test_unique_submission(peer_client):
    """
    Tests that we only successfully send a submit only once
    """
    peer, client = peer_client
    task = gts.sleeps(1, 42)
    tdef = task.task_def

    submit_counter = [0]
    stat_counter = [0]
    def _count(*_):
        submit_counter[0] += 1
    def _count_stat(*_):
        stat_counter[0] += 1
    peer.protocol.on_submit = _count
    peer.protocol.on_stat = _count_stat

    bg_collect = asyncio.create_task(
        client.collect(task)
        )
    try:
        async with timeout(6):
            # Process one STAT for the task, two for the args and reply NOTFOUND
            for name in (task.name, tdef.args[0].name, tdef.args[1].name):
                await peer.recv_message()
                await peer.send_message(
                    peer.protocol.not_found(
                        peer.protocol.default_route(),
                        name
                        )
                    )

            # Process one SUBMIT and drop it
            await peer.recv_message()
            logging.info('Mock dropping')
            # Process a second SUBMIT and reply DOING
            await peer.recv_message()
            logging.info('Mock processing')
            await peer.send_message(
                Doing.plain_reply(
                    peer.protocol.default_route(),
                    name=task.name # pylint: disable=no-member
                    )
                )
            # We should not receive any further message, at least until we add status
            # update to the protocol
            with pytest.raises(asyncio.TimeoutError):
                async with timeout(2):
                    await peer.recv_message()
    finally:
        with pytest.raises(asyncio.CancelledError):
            bg_collect.cancel()
            await bg_collect

    # 1 drop + 1 through
    assert submit_counter[0] == 2
