"""
Fixtures related to creating galp clients
"""
import pytest

import galp

@pytest.fixture
def client(galp_set_one):
    """A client connected to a pool with one worker through a broker"""
    return galp_set_one.client

@pytest.fixture
def client_pool(galp_set_many):
    """A client connected to a pool of 10 workers through a broker"""
    return galp_set_many.client

@pytest.fixture
def client_pair(galp_set_one):
    """A pair of clients connected to the same pool of one worker through a broker.

    This is now implemented with two incoming connections to the same broker,
    routing on broker/worker-side should disantangle things.
    """
    return galp_set_one.client, galp.Client(galp_set_one.endpoint)

@pytest.fixture
async def disjoined_client_pair(make_galp_set):
    """A pair of clients connected to two different sets of galp processes with
    one worker in each"""
    gls1 = await make_galp_set(1)
    gls2 = await make_galp_set(1)
    return gls1.client, gls2.client
