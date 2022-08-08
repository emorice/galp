"""
All-in-one client, broker, worker
"""

import os
from contextlib import asynccontextmanager
import argparse

from galp.client import Client
from galp.broker import Broker
from galp.pool import Pool
from galp.async_utils import background

@asynccontextmanager
async def local_system(pool_size=1, steps=[]):
    """
    Starts a broker and pool asynchronously, yields a client.

    Only supports one simulateneous call per program
    """
    client_endpoint = b'inproc://galp_cl'
    worker_endpoint = f'ipc://@galp_wk_{os.getpid()}'.encode('ascii')

    broker = Broker(
        worker_endpoint=worker_endpoint,
        client_endpoint=client_endpoint
        )
    pool = Pool(argparse.Namespace(
            endpoint=worker_endpoint,
            pool_size=pool_size,
            config_dict={'steps': steps},
            debug=True,
            ))

    async with background(broker.run()):
        async with background(pool.run()):
            yield Client(client_endpoint)
