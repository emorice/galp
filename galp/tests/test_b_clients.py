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

@pytest.fixture
async def blocked_client():
    """
    A Client and bound socket with the client sending queue artificially filled
    """
    # Inproc seems to have the most consistent buffering behavior,
    # understandably
    endpoint = 'inproc://test_fill_queue'
    peer = ZmqAsyncTransport(
        Protocol('CL', router=False),
        endpoint, zmq.DEALER, bind=True) # pylint: disable=no-member

    client = galp.Client(endpoint)

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

    peer.socket.close(linger=1)
    client.transport.socket.close(linger=1)

async def test_fill_queue(blocked_client):
    """
    Tests that we can saturate the client outgoing queue at will
    """
    _, client = blocked_client
    task = gts.plugin_hello()
    route = client.proto.default_route()

    # Check that client blocks
    with pytest.raises(asyncio.TimeoutError):
        async with timeout(1):
            await client.transport.send_message(
                client.proto.submit_task(route, task)
                )
