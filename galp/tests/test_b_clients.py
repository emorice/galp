"""
Tests of client behavior, not connected to a full broker or worker
"""

import asyncio
import logging
import zmq

import pytest

import galp
import galp.tests.steps as gts
from galp.zmq_async_protocol import ZmqAsyncProtocol

from async_timeout import timeout

@pytest.fixture
async def blocked_client(async_ctx):
    """
    A Client and bound socket with the client sending queue artificially filled
    """
    # Inproc seems to have the most consistent buffering behavior,
    # understandably
    endpoint = 'inproc://test_fill_queue'
    peer = ZmqAsyncProtocol('B', endpoint, zmq.DEALER, bind=True)

    client = galp.Client(endpoint)

    # Lower the HWMs first, else filling the queues takes lots of messages
    # 0 creates problems, 1 seems the minimum still safe
    client.socket.hwm = 1
    peer.socket.hwm = 1

    fillers = [0]
    async def _fill():
        await client.socket.send(b'FILLER', flags=zmq.NOBLOCK)
        fillers[0] += 1

    with pytest.raises(zmq.ZMQError):
        # Send messages until we error or timeout to fill the queue
        # If this work zmq should eventually raise
        async with timeout(1):
            while True:
                await _fill()
    logging.warning('Queue filled after %d fillers', fillers[0])

    # Check that we can reliably not send anything more
    for i in range(100):
        with pytest.raises(zmq.ZMQError):
            await _fill()

    yield peer, client

    peer.socket.close(linger=1)
    client.socket.close(linger=1)

async def test_fill_queue(blocked_client):
    """
    Tests that we can saturate the client outgoing queue at will
    """
    peer, client = blocked_client
    task = gts.plugin_hello()
    route = client.default_route()

    # Check that client blocks
    with pytest.raises(asyncio.TimeoutError):
        async with timeout(1):
            await super(galp.Client, client).submit_task(route, task)

@pytest.mark.xfail
async def test_blocked_done(blocked_client):
    """Tests that a blocked client can still process messages without blocking"""

    peer, client = blocked_client

    # Set up two tasks such that marking the first as done should trigger
    # scheduling the second
    upstream_task = gts.arange(5)
    downstream_task = gts.npsum(upstream_task)

    # This should schedule the first task,
    # then wait forever for the submit to get through.
    bg = asyncio.create_task(client.collect(downstream_task))

    # Wait until the client graph is built
    async with timeout(1):
        while len(client._details) != 3:
            await asyncio.sleep(0.01)

    # Check that we would handle a DONE without blocking
    async with timeout(1):
        await client.on_done(client.default_route(), upstream_task.name)

    bg.cancel()
    with pytest.raises(asyncio.CancelledError):
        await bg
