"""
Steps involving more than the simplests graphs
"""
from functools import partial

import pytest
from async_timeout import timeout

import galp
import galp.tests.steps as gts

@pytest.fixture
def run(tmpdir):
    """
    Bind test parameters for galp.run
    """
    return partial(galp.run, store=tmpdir, steps=['galp.tests.steps'],
            log_level='info')

async def assert_task_equal(task, result, client):
    """
    Wraps equality assertion against client return in timeout guard
    """
    async with timeout(3):
        assert result == await client.run(task)

@pytest.mark.xfail
async def test_meta_step(client):
    """
    Task returning a structure of other tasks to run recursively
    """
    await assert_task_equal(
            gts.meta((1, 2, 3)),
            (1, 2, 3),
            client)

@pytest.mark.xfail
def test_resume_meta(run):
    """
    Run a meta-task with a failing child twice to force resumed collection
    """
    meta_fail = gts.meta_error

    with pytest.raises(galp.TaskFailedError):
        run(meta_fail)

    with pytest.raises(galp.TaskFailedError):
        run(meta_fail)
