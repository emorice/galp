"""
Handle various common runtime failures nicely
"""

import os
import asyncio
import signal
import cProfile

import psutil

import pytest
from async_timeout import timeout

import galp
import tests.steps as gts

# Other fixtures
# ========

@pytest.fixture(params=[
    # Invalid children value
    bytes('INVALID', 'ascii'),
    # Valid children value
    bytes.fromhex('90')
    ])
def poisoned_cache(tmpdir, request):
    """
    A galp cache with a random bad value in it

    The error should occur when deserializing the children list, so rpesumably
    worker-side
    """
    task = gts.arange(5)

    # Valid name
    name = task.name
    # Either children value
    children = request.param
    # Invalid data but we should fail before that
    data = bytes.fromhex('0123456789abcdef')

    cache = galp.store.Store(tmpdir, None)
    cache.serialcache[name + b'.children'] = children
    cache.serialcache[name + b'.data'] = data

    return task

async def test_step_error(client):
    """
    Test running a task containing a bug, and test that the error message is
    somewhat helpful.
    """
    with pytest.raises(galp.TaskFailedError) as exc:
        await asyncio.wait_for(client.collect(gts.raises_error()), 3)
    # ensure it's written somewhere that this is not a client-side error
    assert 'worker' in str(exc).lower()

async def test_suicide(client):
    """
    Test running a task triggering a signal
    """
    with pytest.raises(galp.TaskFailedError):
        await asyncio.wait_for(client.collect(gts.suicide(signal.SIGKILL)), 3)

async def test_step_error_multiple(client):
    """
    Test running a multiple task containing a bug
    """
    with pytest.raises(galp.TaskFailedError):
        await asyncio.wait_for(
                client.collect(
                    *gts.raises_error_multiple(),
                    verbose=True),
                3)

async def test_missing_step_error(client):
    """
    Test running a unexisting step
    """

    # Define a step locally that is invisible to the worker
    # (because the worker would need to run this exact function to even
    # reproduce the step function)
    @galp.step
    def missing():
        pass

    with pytest.raises(galp.TaskFailedError):
        await asyncio.wait_for(client.collect(missing()), 3)

@pytest.mark.parametrize('sig', [signal.SIGINT, signal.SIGTERM])
async def test_signals_busyloop(galp_set_one, sig):
    """Tests that worker terminate on signal when stuck in busy loop"""
    client = galp_set_one.client
    pool_handle = galp_set_one.pool

    # This is only supposed to kill the pool and its two children (proxy, worker)
    handles = [pool_handle]

    task = gts.busy_loop()

    background = asyncio.create_task(client.collect(task))

    #FIXME: we should wait for the DOING message instead
    await asyncio.sleep(1)

    # List all processes and children
    # Under the current implementation we should have started one pool manager
    # its forked proxy and one worker
    processes = [
        psutil.Process(handle.pid)
        for handle in handles
        ]
    children = []
    for process in processes:
        assert process.status() != psutil.STATUS_ZOMBIE
        children.extend(process.children(recursive=True))
    assert len(processes + children) == 3

    # Send signal to the pool manager
    for handle in handles:
        handle.send_signal(sig)

    # Check everyone died
    _gone, alive = psutil.wait_procs(processes + children, timeout=4)
    assert not alive

    background.cancel()
    try:
        await background
    except asyncio.CancelledError:
        pass

async def test_return_exceptions(client):
    """
    Test keeping on collecting tasks after first failure
    """
    task_fail = gts.raises_error()
    task_ok = gts.hello()

    # Ensure the first task fails before we run the second
    with pytest.raises(galp.TaskFailedError):
        ans_fail, = await asyncio.wait_for(client.collect(task_fail), 3)

    ans_fail, ans_ok = await asyncio.wait_for(
        client.collect(task_fail, task_ok, return_exceptions=True),
        3)

    assert isinstance(ans_fail, galp.TaskFailedError)
    assert ans_ok == gts.hello.function() # pylint: disable=no-member

async def test_cache_corruption_get(poisoned_cache, client):
    """
    Raise a TaskFailed if we cannot fetch a corrupted entry
    """
    task = poisoned_cache

    with pytest.raises(galp.TaskFailedError):
        ans, = await asyncio.wait_for(
            client.collect(task),
            3)

    # Test that the worker recovered
    task3 = gts.hello()
    ans, = await asyncio.wait_for(
        client.collect(task3),
        3)

    assert ans == gts.hello.function()

async def test_remote_cache_corruption(poisoned_cache, client):
    """
    Raise a TaskFailed if we cannot re-use a corrupted entry as input
    """
    task = poisoned_cache

    task2 = gts.npsum(task)

    with pytest.raises(galp.TaskFailedError):
        ans, = await asyncio.wait_for(
            client.collect(task2, verbose=True),
            3)

    # Test that the worker recovered
    task3 = gts.hello()
    ans, = await asyncio.wait_for(
        client.collect(task3, verbose=True),
        3)

    assert ans == gts.hello.function()

async def test_refcount(client):
    """
    Test that resources used by a failed tasks are cleaned up before the next
    one executes
    """
    obj1, obj2 = gts.RefCounted(), gts.RefCounted()
    assert obj1.last_count == 0
    assert obj2.last_count == 1

    fail_tasks = [
        gts.refcount(i, fail=True)
        for i in range(2)
        ]
    ok_task = gts.refcount(0, fail=False)

    fails = await asyncio.wait_for(
        client.collect(*fail_tasks, return_exceptions=True),
        3)
    assert all(isinstance(fail, galp.TaskFailedError) for fail in fails)

    ok, = await asyncio.wait_for(
        client.collect(ok_task, return_exceptions=True),
        3)

    assert ok == 1

async def test_vmlimit(make_galp_set):
    """
    Worker  fails tasks if going above virtual memory limit
    """
    unlimited_gls = await make_galp_set(1)
    limited_gls = await make_galp_set(1, extra_pool_args={'vm': '2G'})

    unlimited_client = unlimited_gls.client
    limited_client = limited_gls.client

    task_a, task_b = [gts.alloc_mem(2**30, x) for x in 'ab']

    await asyncio.wait_for(
            unlimited_client.collect(task_a),
            3)

    with pytest.raises(galp.TaskFailedError):
        await asyncio.wait_for(
                limited_client.collect(task_b),
                3)

async def test_missing_argument(client):
    """
    Raises early when missing arguments
    """
    async with timeout(3):
        # This must raise with a TypeError at graph construction time and not a
        # TaskFailedError at runtime.
        with pytest.raises(TypeError) as err:
            await client.run(gts.arange, dry_run=True)
        print(err)

async def test_rerun_local(client, tmp_path):
    """
    Re-run a failed task in current interpreter
    """
    zero_task = gts.divide(0, den=1) # 0 / 1, ok
    bad_task = gts.divide(1, zero_task) # 1 / 0, error

    async with timeout(3):
        ret, = await client.collect(zero_task)
        assert ret == 0.
        with pytest.raises(galp.TaskFailedError) as exc:
            await client.collect(bad_task)
        # ensure the full task name is printed out
        assert bad_task.name.hex() in str(exc)

    # Good task, inspect args and reproduce result
    run_ok = galp.prepare_task(zero_task.name, store_path=str(tmp_path))
    assert run_ok.args == (0,)
    assert run_ok.keywords == {'den': 1}
    assert run_ok() == 0.0

    # Bad task, inspect args and reproduce error
    run_bad = galp.prepare_task(bad_task.name, store_path=str(tmp_path))
    assert run_bad.args == (1, 0.0)
    assert run_bad.keywords == {}
    with pytest.raises(ZeroDivisionError):
        run_bad()

async def test_rerun_profile(client, tmp_path):
    """
    Re-run a task and profile it

    This is arguably not really a test and more of a demo
    """
    task = gts.profile_me(27)

    async with timeout(3):
        ret, = await client.collect(task)
        assert ret == 196418

    run_it = galp.prepare_task(task.name, store_path=str(tmp_path))
    cProfile.runctx('run()', globals(), {'run': run_it})

async def test_logfiles(tmp_path, client):
    """
    Log files are created per task for forensics
    """
    task = gts.echo(
            to_stdout='This goes to stdout',
            to_stderr='And that goes to stderr'
            )

    async with timeout(3):
        ret, = await client.collect(task)
        assert ret is None

    with open(os.path.join(
        tmp_path, 'logs', f'{task.name.hex()}.out'
        ), 'r', encoding='utf8') as log_out:
        assert log_out.read() == 'This goes to stdout\n'

    with open(os.path.join(
        tmp_path, 'logs', f'{task.name.hex()}.err'
        ), 'r', encoding='utf8') as log_err:
        assert log_err.read() == 'And that goes to stderr\n'
