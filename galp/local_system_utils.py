"""
All-in-one client, broker, worker
"""

import os
import tempfile
import logging

from contextlib import asynccontextmanager, AsyncExitStack, contextmanager

import galp.pool
from galp.client import Client
from galp.broker import broker_serve
from galp.async_utils import background

class LocalSystem:
    """
    Asynchronous exit stack encapsulating broker, pool and client management

    Tries to pass on the current default log level to the forked processes.
    """

    _instances = 0 # To generate unique endpoint names

    def __init__(self, pool_size=1, **worker_options):
        self._stack = AsyncExitStack()

        endpoint = f'ipc://@galp_{os.getpid()}_{self._instances}'
        LocalSystem._instances += 1
        self.endpoint = endpoint

        self._broker_config = {
                'endpoint': endpoint,
                'n_cpus': pool_size
                }
        self._pool_config = {
                'endpoint': endpoint,
                **worker_options,
            }
        if 'log_level' not in self._pool_config:
            self._pool_config['log_level'] = logging.getLogger().level

        self.pool = None
        self.client = None

    async def start(self) -> Client:
        """
        Starts a load-balancer and pool manager in the background, returns the
        corresponding client.

        If a setup step fails, you must call `stop` anyway to ensure proper
        cleanup.
        """
        await self._stack.enter_async_context(
            background(broker_serve(**self._broker_config))
            )
        self.pool = self._stack.enter_context(
            _spawn_pool(self._pool_config)
            )
        self.client =  Client(self.endpoint)
        return self.client

    async def stop(self) -> None:
        """
        Stops background tasks and clean up
        """
        self.client = None
        await self._stack.aclose()

@contextmanager
def _spawn_pool(config):
    popen = galp.pool.spawn(config)
    yield popen
    popen.terminate()
    popen.wait()

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
