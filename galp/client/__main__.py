"""
Command line interface to the galp client
"""

import argparse
import asyncio
import logging

from contextlib import AsyncExitStack
from importlib import import_module

import galp
import galp.cache
import galp.cli

parser = argparse.ArgumentParser()
parser.add_argument('module', help='python module containing the target')
parser.add_argument('target', help='attribute of the module to use as main task')

parser.add_argument('-e', '--endpoint', help='zmq endpoint of the broker. '
        'If not given, a local galp system is started')
parser.add_argument('-q', '--quiet', action='store_true',
        help='do nor print the result on the standard output')
parser.add_argument('-n', '--dry-run', action='store_true',
        help='do not actually run the tasks, just print what would be done')
parser.add_argument('-j', '--jobs', type=int, help='Number of worker processes'
        ' to run in parallel. Ignored with -e.', default=1, dest='pool_size')
galp.cache.add_store_argument(parser, optional=True)
galp.cli.add_parser_arguments(parser)

args = parser.parse_args()

galp.cli.setup('client', args.log_level)

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
                galp.local_system(**{
                    k: getattr(args, k)
                    for k in ['config', 'log_level', 'store', 'pool_size']
                    })
                )
        return await client.run(target, dry_run=args.dry_run)

result = asyncio.run(
        run(_target)
        )

if not (args.quiet or args.dry_run):
    print(result)
