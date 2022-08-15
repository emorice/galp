"""
All-in-one client, broker, worker
"""

import os
import tempfile

from contextlib import asynccontextmanager, AsyncExitStack

from galp.client import Client
from galp.broker import Broker
from galp.pool import Pool
from galp.async_utils import background

class LocalSystem:
    """
    Asynchronous exit stack encapsulating broker, pool and client management
    """
    def __init__(self, pool_size=1, steps=None):
        self._stack = AsyncExitStack()

        self._client_endpoint = b'inproc://galp_cl'
        worker_endpoint = f'ipc://@galp_wk_{os.getpid()}'.encode('ascii')

        self._broker = Broker(
            worker_endpoint=worker_endpoint,
            client_endpoint=self._client_endpoint
            )
        self._pool_config = {
                'pool_size': pool_size,
                'endpoint': worker_endpoint
            }
        self._worker_config = {
                'endpoint': worker_endpoint,
                'steps': steps,
            }

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
        _pool = Pool(
            self._pool_config,
            self._worker_config
            )
        await self._stack.enter_async_context(
            background(_pool.run())
            )
        self.client =  Client(self._client_endpoint)
        return self.client

    async def stop(self):
        """
        Stops background tasks and clean up
        """
        self.client = None
        await self._stack.aclose()

class TempSystem(LocalSystem):
    """
    Manages a temporary directory for LocalSystem
    """
    async def start(self):
        tmpdir = self._stack.enter_context(
            tempfile.TemporaryDirectory()
            )
        self._worker_config['store'] = tmpdir
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
