"""
All-in-one client, broker, worker
"""

import os
import signal
import tempfile
import logging

from contextlib import asynccontextmanager, AsyncExitStack, contextmanager

import galp.pool
from galp.client import Client
from galp.broker import Broker
from galp.async_utils import background
from galp.cli import run_in_fork

class LocalSystem:
    """
    Asynchronous exit stack encapsulating broker, pool and client management

    Tries to pass on the current default log level to the forked processes.
    """
    def __init__(self, pool_size=1, pin_workers=False, **worker_options):
        self._stack = AsyncExitStack()

        endpoint = f'ipc://@galp_wk_{os.getpid()}'.encode('ascii')
        self.endpoint = endpoint

        self._broker = Broker(
            endpoint=endpoint,
            n_cpus=pool_size
            )
        self._pool_config = {
                'endpoint': endpoint,
                'pin_workers': pin_workers,
                **worker_options,
            }
        if 'log_level' not in self._pool_config:
            self._pool_config['log_level'] = logging.getLogger().level

        self.client = None

    async def start(self):
        """
        Starts a load-balancer and pool manager in the background, returns the
        corresponding client.

        If a setup step fails, you must call `stop` anyway to ensure proper
        cleanup.
        """
        await self._stack.enter_async_context(
            background(self._broker.run())
            )
        self._stack.enter_context(
            _fork_pool(self._pool_config)
            )
        self.client =  Client(self.endpoint)
        return self.client

    async def stop(self):
        """
        Stops background tasks and clean up
        """
        self.client = None
        await self._stack.aclose()

@contextmanager
def _fork_pool(config):
    pid = run_in_fork(galp.pool.main, config)
    yield
    os.kill(pid, signal.SIGTERM)
    os.waitpid(pid, 0)

class TempSystem(LocalSystem):
    """
    Manages a temporary directory for LocalSystem
    """
    async def start(self):
        tmpdir = self._stack.enter_context(
            tempfile.TemporaryDirectory()
            )
        self._pool_config['store'] = tmpdir
        return await super().start()

@asynccontextmanager
async def local_system(*args, **kwargs):
    """
    Starts a broker and pool asynchronously, yields a client.

    See LocalSystem for supported arguments.

    Only supports one simulateneous call per program
    """
    gls = LocalSystem(*args, **kwargs)
    try:
        yield await gls.start()
    finally:
        await gls.stop()

@asynccontextmanager
async def temp_system(*args, **kwargs):
    """
    Starts a broker and pool asynchronously, yields a client.

    See TempSystem for supported arguments.

    Only supports one simulateneous call per program
    """
    gls = TempSystem(*args, **kwargs)
    try:
        yield await gls.start()
    finally:
        await gls.stop()
