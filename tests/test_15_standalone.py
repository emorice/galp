"""
Tests running galp tasks on a locally created galp system with minimum setup
"""

import signal

import asyncio
from async_timeout import timeout

import pytest

import galp
import tests.steps as gts

# pylint: disable=no-member,redefined-outer-name

async def test_standalone():
    """
    Use a one-liner to start galp and use it
    """
    task = gts.hello()

    async with timeout(3):
        async with galp.temp_system() as client:
            res = await client.run(task)
        assert res == gts.hello.function()

async def test_standalone_jobs():
    """
    Temp system with several jobs.
    """
    task = gts.hello()

    async with timeout(3):
        async with galp.temp_system(pool_size=2) as client:
            res = await client.run(task)
        assert res == gts.hello.function()

async def test_explicit():
    """
    Use a standalone system without the context manager syntax
    """
    task = gts.identity(1234)

    gls = galp.TempSystem()

    async with timeout(3):
        client = await gls.start()
        res = await client.run(task)
        assert res == 1234

        client2 = gls.client
        res = await client2.run(task)
        assert res == 1234

        await gls.stop()

        assert gls.client is None

@pytest.fixture
def run(tmpdir):
    """
    Run a task (with a timeout)
    """
    return lambda task, **kwargs: galp.run(task,
        store=tmpdir, timeout=3, **kwargs)

def test_oneshot(run):
    """
    Run a task through an all-in-one wrapper.
    """
    assert run(gts.identity(1234)) == 1234

def test_oneshot_missing_store():
    """
    Get a helpful message if you forget the store
    """
    with pytest.raises(TypeError):
        galp.run(gts.identity(1234))

def test_oneshot_verbose(run, capsys):
    """
    Supports the verbose keyword
    """
    assert run(gts.identity(1234)) == 1234
    out = capsys.readouterr().out
    assert 'OK' not in out

    # Need a different task as a cache hit is silent
    assert run(gts.identity(1235), verbose=True) == 1235
    out = capsys.readouterr().out
    assert 'OK' in out

def test_oneshot_timeout(run):
    """
    Raise if the task never completes
    """
    with pytest.raises(asyncio.TimeoutError):
        run(gts.busy_loop())

def test_oneshot_dryrun(run):
    """
    Dry-run a task through an all-in-one wrapper.
    """
    assert run(gts.identity(1234), dry_run=True) is None

def test_oneshot_suicide(run):
    """
    Run a task that will cause worker crash
    """
    with pytest.raises(galp.TaskFailedError):
        run(gts.suicide(signal.SIGKILL))

def test_definition_cpus(run):
    """
    Run two tasks defined with a different number of cpus
    """
    one = gts.utils.get_cpus()
    with galp.resources(cpus=2):
        two = gts.utils.get_cpus()
    assert run([one, two], pool_size=2, pin_workers=True) == (1, 2)
