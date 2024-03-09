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
import galp.task_types as gtt
from galp.protocol import make_stack
from galp.zmq_async_transport import ZmqAsyncTransport
from galp.net.core.types import Reply, RequestId, Submit, Stat, NextRequest
from galp.net.requests.types import Doing, NotFound

# pylint: disable=redefined-outer-name

@pytest.fixture
async def make_peer_client():
    """
    A client and connected peer
    """
    # Inproc seems to have the most consistent buffering behavior,
    # understandably
    endpoint = 'inproc://test_fill_queue'
    peers, clients = [], []
    def _make_peers(handler):
        peer = ZmqAsyncTransport(
                make_stack(handler,
                    'CL'),
            endpoint, zmq.DEALER, bind=True) # pylint: disable=no-member
        peers.append(peer)

        client = galp.Client(endpoint)
        clients.append(client)
        return peer, client

    yield _make_peers

    for peer in peers:
        peer.socket.close(linger=1)
    for client in clients:
        client.transport.socket.close(linger=1)

@pytest.fixture
def make_blocked_client(make_peer_client):
    """
    A Client and bound socket with the client sending queue artificially filled
    """
    async def _make(handler):
        peer, client = make_peer_client(handler)

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

        return peer, client

    yield _make

async def test_fill_queue(make_blocked_client):
    """
    Tests that we can saturate the client outgoing queue at will
    """
    _, client = await make_blocked_client(lambda *_: [])
    task = gts.hello()

    # Check that client blocks
    with pytest.raises(asyncio.TimeoutError):
        async with timeout(1):
            await client.transport.send_message(
                    # pylint: disable=no-member
                    Submit(task_def=task.task_def,
                           resources=gtt.Resources(cpus=1))
                    )

async def test_unique_submission(make_peer_client):
    """
    Tests that we only successfully send a submit only once
    """
    task = gts.sleeps(1, 42)
    # pylint: disable=no-member
    tdef = task.task_def

    submit_counter = [0]
    stat_counter = [0]
    def on_message(_, msg):
        match msg:
            case Submit():
                submit_counter[0] += 1
            case Stat():
                stat_counter[0] += 1
        return []
    peer, client = make_peer_client(on_message)

    bg_collect = asyncio.create_task(
        client.collect(task)
        )
    try:
        async with timeout(6):
            # Process one STAT for the task, two for the args and reply NOTFOUND
            for name in (task.name, tdef.args[0].name, tdef.args[1].name):
                await peer.recv_message()
                await peer.send_message(Reply(RequestId(b'stat', name), NotFound()))
                await peer.send_message(NextRequest())

            # Process one SUBMIT and drop it
            # Note: disabled after switch to NextRequest-driven queue that doesn't drop
            # await peer.recv_message()
            # logging.info('Mock dropping')

            # Process a second SUBMIT and reply DOING
            await peer.recv_message()
            logging.info('Mock processing')
            await peer.send_message(NextRequest())
            await peer.send_message(Reply(RequestId(b'submit', task.name), Doing())) # pylint: disable=no-member

            # We should not receive any further message, at least until we add status
            # update to the protocol
            with pytest.raises(asyncio.TimeoutError):
                async with timeout(2):
                    await peer.recv_message()
    finally:
        with pytest.raises(asyncio.CancelledError):
            bg_collect.cancel()
            await bg_collect

    assert submit_counter[0] == 1
