"""
Command line interface to the galp client
"""

import argparse
import asyncio

from contextlib import AsyncExitStack
from importlib import import_module

import galp

parser = argparse.ArgumentParser()
parser.add_argument('module', help='python module containing the target')
parser.add_argument('target', help='attribute of the module to use as main task')

parser.add_argument('-e', '--endpoint', help='zmq endpoint of the broker. '
        'If not given, a local galp system is started')
parser.add_argument('-q', '--quiet', action='store_true',
        help='do nor print the result on the standard output')

args = parser.parse_args()

module = import_module(args.module)
_target = getattr(module, args.target)

async def run(target):
    """
    Create a client and run the pipeline
    """
    async with AsyncExitStack() as stack:
        if args.endpoint:
            client = galp.Client(args.endpoint)
        else:
            client = await stack.enter_async_context(
                galp.local_system()
                )
        return await client.run(target)

result = asyncio.run(
        run(_target)
        )

if not args.quiet:
    print(result)
