"""
Tests relating to brokers and borker parts in isolation
"""

import pytest
from async_timeout import timeout

from galp.broker.broker import CommonProtocol
from galp.task_types import Resources

@pytest.fixture
def common_protocol():
    """
    The part of a Broker that filters messages passing through the worker side
    """
    return CommonProtocol('WK', router=True, resources=Resources(cpus=0))

async def test_worker_protocol_drops_unaddressed(common_protocol):
    """
    If no workers are available, submit messages should get dropped
    """

    # Empty forward route signals we want any worker
    route = ( [ b'some_client'], [] )
    # The message will actually still get parsed, so we need to make some effort
    body = [b'STAT', b'something32characterslongorsomet']

    async with timeout(3):
        out_messages = common_protocol.on_verb(route, body)

    assert out_messages is None
