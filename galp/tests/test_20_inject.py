"""
Tests related to the dependency injection API
"""

from async_timeout import timeout

import galp.tests.steps.inject as gts

async def test_inject(client):
    """
    Run a task with injected input
    """
    async with timeout(3):
        ans = await client.run(gts.uses_inject)
    assert ans == 'Injected hi'

async def test_inject_bind(client):
    """
    Run a task with an explicitely injected input
    """
    async with timeout(3):
        ans = await client.run(gts.sum_inject)
    assert ans == 12

async def test_inject_transitive(client):
    """
    Run a task, providing an argument to an injected dependency in the call
    """
    task = gts.uses_inject_trans(injected_list_trans=[1, 2, 3])

    async with timeout(3):
        ans = await client.run(task)
    assert ans == 6

async def test_inject_none(client):
    """
    Run a task, providing an argument to an injected dependency in the call
    """
    task = gts.injects_none

    async with timeout(3):
        ans = await client.run(task)
    assert ans is True
