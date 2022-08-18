"""
Steps involving more than the simplests graphs
"""
from async_timeout import timeout

import galp.tests.steps as gts

async def assert_task_equal(task, result, client):
    """
    Wraps equality assertion against client return in timeout guard
    """
    async with timeout(3):
        assert result == await client.run(task)

async def test_meta_step(client):
    """
    Task returning a structure of other tasks to run recursively
    """
    await assert_task_equal(
            gts.meta((1, 2, 3)),
            (1, 2, 3),
            client)
