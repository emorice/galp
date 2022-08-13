"""
Tests running galp tasks on a locally created galp system with minimum setup
"""

from async_timeout import timeout

import galp
import galp.tests.steps as gts

# pylint: disable=no-member

async def test_standalone():
    """
    Use a one-liner to start galp and use it
    """
    task = gts.plugin_hello()

    async with timeout(3):
        async with galp.local_system(steps=['galp.tests.steps']) as client:
            res = await client.run(task)
        assert res == task.step.function()

async def test_explicit():
    """
    Use a standalone system without the context manager syntax
    """
    task = gts.identity(1234)

    gls = galp.LocalSystem(steps=['galp.tests.steps'])

    async with timeout(3):
        client = await gls.start()
        res = await client.run(task)
        assert res == 1234
        await gls.stop()
