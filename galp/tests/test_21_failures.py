"""
Handle various common runtime failures nicely
"""

import asyncio
import signal

import psutil

import pytest

import galp
import galp.tests.steps as gts

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

    cache = galp.cache.CacheStack(tmpdir, None)
    cache.serialcache[name + b'.children'] = children
    cache.serialcache[name + b'.data'] = data

    return task

async def test_step_error(client):
    """
    Test running a task containing a bug, and test that the error message is
    somewhat helpful.
    """
    with pytest.raises(galp.TaskFailedError):
        try:
            await asyncio.wait_for(client.collect(gts.raises_error()), 3)
        except galp.TaskFailedError as exc:
            # ensure it's written somewhere that this is not a client-side error
            assert 'worker' in str(exc).lower()
            raise

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
        await asyncio.wait_for(client.collect(*gts.raises_error_multiple()), 3)

async def test_missing_step_error(client):
    """
    Test running a unexisting step
    """

    # Define a step locally that is invisible to the worker
    local_export = galp.Block()
    @local_export.step
    def missing():
        pass

    with pytest.raises(galp.TaskFailedError):
        await asyncio.wait_for(client.collect(missing()), 3)

@pytest.mark.parametrize('sig', [signal.SIGINT, signal.SIGTERM])
async def test_signals_busyloop(client, galp_set_one, sig):
    """Tests that worker terminate on signal when stuck in busy loop"""
    client_endoint, all_handles = galp_set_one
    broker_handle, pool_handle = all_handles

    # This is only supposed to kill the pool and its one child
    handles = [pool_handle]

    task = gts.busy_loop()

    bg = asyncio.create_task(client.collect(task))

    #FIXME: we should wait for the DOING message instead
    await asyncio.sleep(1)

    # List all processes and children
    # Under the current implementation we should have started one pool manager
    # and one worker
    processes = [
        psutil.Process(handle.pid)
        for handle in handles
        ]
    children = []
    for process in processes:
        assert process.status() != psutil.STATUS_ZOMBIE
        children.extend(process.children(recursive=True))
    assert len(processes + children) == 2

    # Send signal to the pool manager
    for handle in handles:
        handle.send_signal(sig)

    # Check everyone died
    gone, alive = psutil.wait_procs(processes + children, timeout=4)
    assert not alive

    bg.cancel()
    try:
        await bg
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
            client.collect(task2),
            3)

    # Test that the worker recovered
    task3 = gts.hello()
    ans, = await asyncio.wait_for(
        client.collect(task3),
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

async def test_vmlimit(make_galp_set, make_client):
    """
    Worker  fails tasks if going above virtual memory limit
    """
    unlimited_ep, _ = make_galp_set(1)
    limited_ep, _ = make_galp_set(1, extra_pool_args=['--vm', '2G'])

    unlimited_client = make_client(unlimited_ep)
    limited_client = make_client(limited_ep)

    task_a, task_b = [gts.alloc_mem(2**30, x) for x in 'ab']

    ans = await unlimited_client.collect(task_a, timeout=3)

    with pytest.raises(galp.TaskFailedError):
        ans = await limited_client.collect(task_b, timeout=3)
