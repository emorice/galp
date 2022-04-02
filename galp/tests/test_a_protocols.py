"""
Smaller tests than test_galp, means to exercise some more subtle aspects of the
protocol handlers that are hard to create reliably in end-to-end tests
"""

import zmq

from galp.tests.with_timeout import with_timeout

from galp.zmq_async_protocol import ZmqAsyncProtocol

@with_timeout()
async def test_counters():
    """
    Test that if a peer processes less messages than received, the sender
    counters detect it.
    """

    endpoint = 'inproc://test'
    socket_type = zmq.DEALER

    queue_size = 10

    peer_a = ZmqAsyncProtocol('A', endpoint, socket_type, bind=True)
    peer_b = ZmqAsyncProtocol('B', endpoint, socket_type, capacity=queue_size)

    name = b'1234'
    route = peer_a.default_route()

    i = 0

    # We need at least one message for B to tell A its capacity

    await peer_b.ping(route)
    await peer_a.process_one()

    while peer_a._next_send_idx < peer_a._next_block_idx and i <= queue_size + 100:
        # In each iterations, send two messages from a for one from b
        await peer_a.get(route, name)
        await peer_b.process_one()

        await peer_a.get(route, name)
        # We do not process this one, simulating b being slow

        await peer_b.put(route, name, b'some_proto', b'some_data', b'\0')
        await peer_a.process_one()

        i += 1

    # We have a queue of size queue_size, and we send one extra message per loop, so we
    # should run the loop exactly queue_size times
    assert i == queue_size

