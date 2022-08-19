"""
Galp, a network-based incremental pipeline runner with tight python integration.
"""
import asyncio

from .local_system_utils import local_system, LocalSystem
from .local_system_utils import temp_system, TempSystem
from .client import Client, TaskFailedError
from .graph import StepSet
from .serializer import DeserializeError

def run(*tasks, **options):
    """
    Start a local system an run the given tasks in it.

    Args:
        any keyword argument to Client.run or LocalSystem
    """

    return asyncio.run(async_run(*tasks, **options))

async def async_run(*tasks, **options):
    """
    Async version of `run`
    """
    run_options = {
        k: opt
        for k, opt in options.items()
        if k in ('return_exceptions', 'timeout')
        }
    for k in run_options:
        del options[k]

    async with local_system(**options) as client:
        return await client.run(*tasks, **run_options)
