"""
All-in-one client, broker, worker
"""

import os
from contextlib import asynccontextmanager, AsyncExitStack
import argparse

from galp.client import Client
from galp.broker import Broker
from galp.pool import Pool
from galp.async_utils import background

class LocalSystem:
    """
    Asynchronous exit stack encapsulating broker, pool and client management
    """
    def __init__(self, pool_size=1, steps=[]):
        self._stack = AsyncExitStack()

        self._client_endpoint = b'inproc://galp_cl'
        worker_endpoint = f'ipc://@galp_wk_{os.getpid()}'.encode('ascii')

        self._broker = Broker(
            worker_endpoint=worker_endpoint,
            client_endpoint=self._client_endpoint
            )
        self._pool = Pool(argparse.Namespace(
                endpoint=worker_endpoint,
                pool_size=pool_size,
                config_dict={'steps': steps},
                ))
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
        await self._stack.enter_async_context(
            background(self._pool.run())
            )
        self.client =  Client(self._client_endpoint)
        return self.client

    async def stop(self):
        """
        Stops background tasks and clean up
        """
        self.client = None
        await self._stack.aclose()


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
