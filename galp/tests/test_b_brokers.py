"""
Tests relating to brokers and borker parts in isolation
"""

import pytest
from async_timeout import timeout

from galp.broker import WorkerProtocol

@pytest.fixture
def worker_protocol():
    return WorkerProtocol()

async def test_worker_protocol_drops_unaddressed(worker_protocol):
    """
    If no workers are available, submit messages should get dropped
    """

    # Empty forward route signals we want any worker
    route = ( [ b'some_client'], [] )
    # The message should not even get parsed
    body = [b'DO', b'something']

    with timeout(3):
        out_messages = worker_protocol.write_message_to(route, body)

    assert len(out_messages) == 0
