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
