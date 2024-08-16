"""
Steps involving more than the simplests graphs
"""
from functools import partial

import pytest
from async_timeout import timeout

import galp
import tests.steps as gts

# pylint: disable=redefined-outer-name

@pytest.fixture
def run(tmpdir):
    """
    Bind test parameters for galp.run
    """
    return partial(galp.run, store=tmpdir, log_level='info')

@pytest.fixture
def assert_task_equal(client):
    """
    Wraps equality assertion against client return in timeout guard
    """
    async def _assert_task_equal(task, result):
        async with timeout(5):
            assert result == await client.run(task, verbose=True)
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
    meta_fail = gts.meta_error()

    with pytest.raises(galp.TaskFailedError):
        run(meta_fail)

    with pytest.raises(galp.TaskFailedError):
        run(meta_fail)

async def test_index_anything(assert_task_equal):
    """
    Subscript a task that was not declared as itemizable
    """
    await assert_task_equal(gts.arange(3)[2], 2)

async def test_double_upload(assert_task_equal):
    """
    Shortcut a second upload.
    """
    hello = gts.hello.function()
    await assert_task_equal(gts.double_upload(), ((hello,), (hello,)))

async def test_index_uses_base(assert_task_equal):
    """
    Index does not require the task to be fully ran yet
    """
    t_dict = gts.trapped_meta()
    t_ok = t_dict['ok']
    t_bad = t_dict['fail']

    await assert_task_equal(t_ok, 1)

    with pytest.raises(galp.TaskFailedError):
        await assert_task_equal(t_bad, None)

    with pytest.raises(galp.TaskFailedError):
        await assert_task_equal(t_dict, None)

async def test_index_meta_meta(assert_task_equal):
    """
    Index follows when the indexed directly refer to an other task to index
    """
    task = gts.meta_trapped_meta()
    t_ok = task['ok']

    await assert_task_equal(t_ok, 1)
