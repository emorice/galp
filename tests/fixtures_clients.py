"""
Fixtures related to creating galp clients
"""
import pytest

import galp

@pytest.fixture
def make_client():
    """Factory fixture for client managing its own sockets"""
    def _make(endpoint):
        return galp.Client(endpoint)
    return _make

@pytest.fixture
async def client(tmp_path):
    """A client connected to a pool with one worker through a broker"""
    async with galp.local_system(store=tmp_path) as the_client:
        yield the_client

@pytest.fixture
async def client_and_handle(tmp_path):
    """A client connected to a pool with one worker through a broker

    Yields:
        (Client, subprocess.Popen)
    """
    system = galp.LocalSystem(store=tmp_path)
    await system.start()
    yield system.client, system.pool
    await system.stop()

@pytest.fixture
def client_pool(make_client, galp_set_many):
    """A client connected to a pool of 10 workers through a broker"""
    endpoint, _ = galp_set_many
    return make_client(endpoint)

@pytest.fixture
def client_pair(galp_set_one, make_client):
    """A pair of clients connected to the same pool of one worker through a broker.

    This is now implemented with two incoming connections to the same broker,
    routing on broker/worker-side should disantangle things.
    """
    endpoint, _ = galp_set_one
    c1 = make_client(endpoint)
    c2 = make_client(endpoint)
    return c1, c2

@pytest.fixture
async def disjoined_client_pair(make_galp_set, make_client):
    """A pair of clients connected to two different sets of galp processes with
    one worker in each"""
    e1, _ = await make_galp_set(1)
    e2, _ = await make_galp_set(1)
    return make_client(e1), make_client(e2)
