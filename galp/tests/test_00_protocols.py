"""
Smaller tests than test_galp, means to exercise some more subtle aspects of the
protocol handlers that are hard to create reliably in end-to-end tests
"""

import zmq

from galp.tests.with_timeout import with_timeout

from galp.zmq_async_transport import ZmqAsyncTransport
from galp.protocol import Protocol
from galp.messages import Put

@with_timeout()
async def test_counters():
    """
    Test that if a peer processes less messages than received, the sender
    counters detect it.
    """

    endpoint = 'inproc://test'
    socket_type = zmq.DEALER # pylint: disable=no-member

    queue_size = 10

    peer_a = ZmqAsyncTransport(
        Protocol('A', router=False),
        endpoint, socket_type, bind=True)

    peer_b = ZmqAsyncTransport(
        Protocol('B', router=False, capacity=queue_size),
        endpoint, socket_type)

    name = b'1234' * 8
    route = peer_a.protocol.default_route()

    i = 0

    # We need at least one message for B to tell A its capacity

    await peer_b.send_message(peer_b.protocol.ping(route))
    await peer_a.recv_message()

    while (peer_a.protocol._next_send_idx < peer_a.protocol._next_block_idx
        and i <= queue_size + 100):
        # In each iterations, send two messages from a for one from b
        await peer_a.send_message(peer_a.protocol.get(route, name))
        await peer_b.recv_message()

        await peer_a.send_message(peer_a.protocol.get(route, name))
        # We do not process this one, simulating b being slow

        await peer_b.send_message(
            peer_b.protocol.put(Put.plain_reply(route, name=name,
                data=b'some_data', children=[]))
            )
        await peer_a.recv_message()

        i += 1

    # We have a queue of size queue_size, and we send one extra message per loop, so we
    # should run the loop exactly queue_size times
    assert i == queue_size
