"""
Fixtures related to creating galp processes to interact with
"""
import os
import logging

from contextlib import (ExitStack, contextmanager, AsyncExitStack,
        asynccontextmanager)

import pytest
import psutil

import galp.worker

@pytest.fixture
def make_worker(tmp_path):
    """Worker fixture, starts a worker in background.

    Returns:
        (endpoint, psutil Process) tuple
    """
    _count = [0]

    @contextmanager
    def _make_one(endpoint=None):
        if endpoint is None:
            endpoint = f'ipc://@galp_test_wk_{os.getpid()}_{_count[0]}'
            _count[0] += 1
        log_level = logging.getLevelName(logging.getLogger().level).lower()

        pid = galp.worker.fork({
            'log_level': log_level,
            'endpoint': endpoint,
            'store': tmp_path})
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
    Start and stop a broker+pool, yields LocalSystem object
    """
    gls = galp.LocalSystem(**kwargs)
    try:
        await gls.start()
        yield gls
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
