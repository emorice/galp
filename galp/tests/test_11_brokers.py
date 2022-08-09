"""
Tests relating to brokers and borker parts in isolation
"""

import pytest
from async_timeout import timeout

from galp.broker.broker import WorkerProtocol

@pytest.fixture
def worker_protocol():
    """
    The part of a Broker that filters messages passing through the worker side
    """
    return WorkerProtocol('WK', router=True)

async def test_worker_protocol_drops_unaddressed(worker_protocol):
    """
    If no workers are available, submit messages should get dropped
    """

    # Empty forward route signals we want any worker
    route = ( [ b'some_client'], [] )
    # The message should not even get parsed
    body = [b'DO', b'something']

    async with timeout(3):
        out_messages = worker_protocol.write_message((route, body))

    assert out_messages is None
