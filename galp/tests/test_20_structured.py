"""
Steps involving more than the simplests graphs
"""
from functools import partial

import pytest
from async_timeout import timeout

import galp
import galp.tests.steps as gts

# pylint: disable=redefined-outer-name

@pytest.fixture
def run(tmpdir):
    """
    Bind test parameters for galp.run
    """
    return partial(galp.run, store=tmpdir, steps=['galp.tests.steps'],
            log_level='info')

@pytest.fixture
def assert_task_equal(client):
    """
    Wraps equality assertion against client return in timeout guard
    """
    async def _assert_task_equal(task, result):
        async with timeout(5):
            assert result == await client.run(task)
    return _assert_task_equal

async def test_meta_step(assert_task_equal):
    """
    Task returning a structure of other tasks to run recursively
    """
    await assert_task_equal(
            gts.meta((1, 2, 3)),
            (1, 2, 3)
            )

async def test_meta_meta_step(assert_task_equal):
    """
    Nested meta steps
    """
    await assert_task_equal(
            gts.meta_meta( ((1, 2, 3), (4, 5)) ),
            ((1, 2, 3), (4, 5))
            )

def test_resume_meta(run):
    """
    Run a meta-task with a failing child twice to force resumed collection
    """
    meta_fail = gts.meta_error

    with pytest.raises(galp.TaskFailedError):
        run(meta_fail)

    with pytest.raises(galp.TaskFailedError):
        run(meta_fail)

async def test_index_anything(assert_task_equal):
    """
    Subscript a task that was declared as itemizable
    """
    await assert_task_equal(gts.arange(3)[2], 2)

async def test_index_step(assert_task_equal):
    """
    Subscript a step with injected parameters
    """
    await assert_task_equal(gts.inject.uses_inject[2], 'j')
    await assert_task_equal(gts.inject.uses_indexed(free_arg='Hello'), 'l')
