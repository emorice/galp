"""
Fixtures related to creating galp processes to interact with
"""
import itertools
import logging
import subprocess
import sys

from contextlib import (ExitStack, contextmanager, AsyncExitStack,
        asynccontextmanager)

import pytest
import psutil

import galp.worker

@pytest.fixture
def port():
    # TODO: these ports could be in use
    ports = itertools.count(48652)

    def _next_port(ports=ports):
        return next(ports)

    return _next_port

def log_level():
    return logging.getLevelName(logging.getLogger().level).lower()

@pytest.fixture
def make_worker(port, tmp_path):
    """Worker fixture, starts a worker in background.

    Returns:
        (endpoint, psutil Process) tuple
    """

    @contextmanager
    def _make_one(endpoint=None):
        if endpoint is None:
            endpoint = f"tcp://127.0.0.1:{port()}"

        pid = galp.worker.fork({
            'config': 'tests/config.toml',
            'log-level': log_level(),
            'endpoint': endpoint,
            'store': str(tmp_path)})
        process = psutil.Process(pid)

        yield endpoint, process

        try:
            process.terminate()
        except psutil.NoSuchProcess:
            pass
        try:
            process.wait()
        except psutil.NoSuchProcess:
            pass

    with ExitStack() as stack:
        def _make(endpoint=None):
            return stack.enter_context(_make_one(endpoint))
        yield _make

@asynccontextmanager
async def _make_set(**kwargs):
    """
    Start and stop a broker+pool, yields endpoint and pool
    """
    gls = galp.LocalSystem(**kwargs)
    try:
        await gls.start()
        yield gls.endpoint, gls.pool
    finally:
        await gls.stop()

@pytest.fixture
async def make_galp_set(tmp_path):
    """
    A set of one broker, one pool manager and up to n connected workers

    Returns:
        (client_endpoint, pool_handle)
    """
    async with AsyncExitStack() as stack:
        async def _make(pool_size, extra_pool_args=None):
            return await stack.enter_async_context(
                _make_set(store=tmp_path, pool_size=pool_size,
                    **(extra_pool_args or {}))
                )
        yield _make

@pytest.fixture
def worker(make_worker):
    """
    A standalone worker
    """
    return make_worker()

@pytest.fixture
async def galp_set_one(make_galp_set):
    """
    A set including a single worker
    """
    return await make_galp_set(1)

@pytest.fixture
async def galp_set_many(make_galp_set):
    """
    A set including at most 10 workers
    """
    return await make_galp_set(10)
