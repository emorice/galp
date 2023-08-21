"""
Tests related to the dependency injection API
"""

import pytest
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
    async with timeout(4):
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

async def test_inject_default(client):
    """
    Run a task with a default argument
    """
    async with timeout(3):
        assert await client.run(gts.uses_has_default) == 42

async def test_inject_nontree(client):
    """
    Run a task with a non-trivial DAG
    """
    async with timeout(4):
        assert await client.run(gts.uses_a_and_ua) is None
        assert await client.run(gts.uses_ua_and_a) is None

async def test_inject_cycle(client):
    """
    Check that we detect cyclic dependencies
    """
    async with timeout(3):
        with pytest.raises(TypeError):
            await client.run(gts.recursive)
        with pytest.raises(TypeError):
            await client.run(gts.cyclic)

async def test_missing_argument(client):
    """
    Detect missing arguments early
    """
    async with timeout(3):
        # This must raise with a TypeError at graph construction time and not a
        # TaskFailedError at runtime.
        with pytest.raises(TypeError):
            await client.run(gts.has_free_arg, dry_run=True)
