"""
Asyncio recipes
"""

import asyncio
from contextlib import asynccontextmanager

@asynccontextmanager
async def background(coroutine):
    """
    Runs a coroutine in background and cancels on exit
    """
    task = asyncio.create_task(coroutine)
    try:
        yield task
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
