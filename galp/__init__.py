"""
Galp, a network-based incremental pipeline runner with tight python integration.
"""
import asyncio

from .local_system_utils import local_system, LocalSystem
from .local_system_utils import temp_system, TempSystem
from .client import Client, TaskFailedError
from .task_types import step, view, query, make_task
from .default_resources import resources
from .context import new_path
from .utils import prepare_task, download

def run(*tasks, **options):
    """
    Start a local system an run the given tasks in it.

    Args:
        any keyword argument to Client.run or LocalSystem,
        also timeout
    """

    return asyncio.run(async_run(*tasks, **options))

async def async_run(*tasks, timeout: int | float | None = None, **options):
    """
    Async version of `run`
    """
    run_options = {}
    for key in ('return_exceptions', 'dry_run', 'verbose'):
        try:
            run_options[key] = options.pop(key)
        except KeyError:
            pass

    async with local_system(**options) as client:
        return await asyncio.wait_for(
                client.run(*tasks, **run_options),
                timeout)
