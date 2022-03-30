"""
Generic tests for galp
"""

import os
import signal
import sys
import subprocess
import logging
import asyncio
import itertools
import signal
import time
import pstats
import psutil

import dill
import zmq
import zmq.asyncio
import pytest
import numpy as np
import pyarrow as pa

import galp.graph
import galp.steps
import galp.client
import galp.synclient
import galp.tests.steps as gts


# Other fixtures
# ========

@pytest.fixture(params=[b'EXIT', b'ILLEGAL'])
def fatal_order(request):
    """All messages that should make the worker quit"""
    return request.param


@pytest.fixture
def poisoned_cache(tmpdir):

    task = gts.arange(5)

    # use a valid name
    name = task.handle.name
    # Use a valid proto name
    proto = galp.serializer.DillSerializer.proto_id
    data = bytes.fromhex('0123456789abcdef')
    children = b'\x00'

    serializer = None

    cache = galp.cache.CacheStack(tmpdir, serializer)
    cache.put_serial(name, proto, data, children)

    return task

# Helpers
# =======

def asserted_zmq_recv_multipart(socket):
    selectable = [socket], [], []
    assert zmq.select(*selectable, timeout=4) == selectable
    return socket.recv_multipart()

def assert_ready(socket):
    """
    Awaits a READY message on the socket
    """
    ans = asserted_zmq_recv_multipart(socket)
    assert len(ans) == 3
    # Third part is a worker self-id
    assert ans[:2] == [b'', b'READY']

# Tests
# =====

def test_nothing():
    """Do not assert anything, just check modules are importable and the
    functionning of the harness itself"""
    pass

def test_shutdown(ctx, worker, fatal_order):
    """Manually send a exit message to a local worker and wait a bit for it to
    terminate."""

    endpoint, worker_handle = worker

    assert worker_handle.poll() is None

    socket = ctx.socket(zmq.DEALER)
    socket.bind(endpoint)
    # Mind the empty frame
    socket.send_multipart([b'', fatal_order])

    assert worker_handle.wait(timeout=4) == 0

    # Note: we only close on normal termination, else we rely on the fixture
    # finalization to set the linger before closing.
    socket.close()

@pytest.mark.parametrize('sig', [signal.SIGINT, signal.SIGTERM])
def test_signals(worker_socket, sig):
    """Test for termination on INT and TERM)"""

    socket, endpoint, worker_handle = worker_socket

    assert worker_handle.poll() is None
    assert_ready(socket)

    process = psutil.Process(worker_handle.pid)
    children = process.children(recursive=True)

    # If not our children list may not be valid
    assert process.status() != psutil.STATUS_ZOMBIE

    worker_handle.send_signal(sig)

    gone, alive = psutil.wait_procs([process, *children], timeout=4)
    assert not alive

@pytest.mark.parametrize('msg', [
    [b'RABBIT'],
    [b'GET'],
    [b'GET', b'one', b'two'],
    [b'SUBMIT'],
    [b'SUBMIT', b'RABBIT'],
    [b'SUBMIT', b'step', b'keyword_without_value'],
    ])
def test_illegals(worker_socket, msg):
    """Tests a few messages that should fire back an illegal

    Note that we should pick some that will not be valid later"""

    socket, *_ = worker_socket

    # Mind the empty frame on both send and receive sides
    socket.send_multipart([b''] + msg)

    assert_ready(socket)

    ans2 = asserted_zmq_recv_multipart(socket)
    assert ans2 == [b'', b'ILLEGAL']

def test_task(worker_socket):
    socket, *_ = worker_socket

    task = galp.steps.galp_hello()
    step_name = task.step.key

    logging.warning('Calling task %s', step_name)

    handle = galp.graph.Task.gen_name(step_name, [], {}, [])

    socket.send_multipart([b'', b'SUBMIT', step_name, b'\x00'])

    assert_ready(socket)

    ans = asserted_zmq_recv_multipart(socket)
    assert ans == [b'', b'DOING', handle]

    ans = asserted_zmq_recv_multipart(socket)
    assert ans == [b'', b'DONE', handle]

    socket.send_multipart([b'', b'GET', handle])

    ans = asserted_zmq_recv_multipart(socket)
    assert ans[:4] == [b'', b'PUT', handle, b'dill']
    assert dill.loads(ans[4]) == 42

def test_notfound(worker_socket):
    """Tests the answer of server when asking to send unexisting resource"""
    worker_socket, *_ = worker_socket

    bad_handle = b'RABBIT'
    worker_socket.send_multipart([b'', b'GET', bad_handle])

    assert_ready(worker_socket)

    ans = asserted_zmq_recv_multipart(worker_socket)
    assert ans == [b'', b'NOTFOUND', bad_handle]

def test_reference(worker_socket):
    """Tests passing the result of a task to an other through handle"""
    worker_socket, *_ = worker_socket

    task1 = galp.steps.galp_double()

    task2 = galp.steps.galp_double(task1)

    worker_socket.send_multipart([b'', b'SUBMIT', task1.step.key, b'\x00'])

    assert_ready(worker_socket)

    # doing
    doing = asserted_zmq_recv_multipart(worker_socket)
    done = asserted_zmq_recv_multipart(worker_socket)
    assert doing, done == ([b'', b'DOING', task1.name], [b'', b'DONE', task1.name])

    worker_socket.send_multipart([b'', b'SUBMIT', task2.step.key, b'\x00', b'', task1.name])

    doing = asserted_zmq_recv_multipart(worker_socket)
    done = asserted_zmq_recv_multipart(worker_socket)
    assert doing, done == ([b'', b'DOING', task2.name], [b'', b'DONE', task2.name])

    # Let's try async get for a twist !
    worker_socket.send_multipart([b'', b'GET', task2.name])
    worker_socket.send_multipart([b'', b'GET', task1.name])

    # Order of the answers is unspecified
    got_a = asserted_zmq_recv_multipart(worker_socket)
    got_b = asserted_zmq_recv_multipart(worker_socket)

    assert got_a[0] == got_b[0] == b''
    assert got_a[1] == got_b[1] == b'PUT'
    assert set((got_a[2], got_b[2])) == set((task1.name, task2.name))
    expected = {
        task1.name: 2,
        task2.name: 4
        }
    assert got_a[3] == got_b[3] == b'dill'
    for _, _, name, proto, res, *children in [got_a, got_b]:
        assert dill.loads(res) == expected[name]

@pytest.mark.asyncio
async def test_async_socket(async_worker_socket):
    sock, _, _ = async_worker_socket

    await asyncio.wait_for(sock.send_multipart([b'', b'RABBIT']), 3)

    ready = await asyncio.wait_for(sock.recv_multipart(), 3)
    assert ready[:2] == [b'', b'READY']

    ans = await asyncio.wait_for(sock.recv_multipart(), 3)
    assert ans == [b'', b'ILLEGAL']

@pytest.mark.asyncio
async def test_client(client):
    """Test simple functionnalities of client"""
    task = galp.steps.galp_hello()

    ans = await asyncio.wait_for(
        client.collect(task),
        3)

    assert ans == [42,]

@pytest.mark.asyncio
async def test_double_collect(client):
    """Proper handling of collecting the same task twice"""
    task = galp.steps.galp_hello()

    ans1 = await asyncio.wait_for(
        client.collect(task),
        3)

    ans2 = await asyncio.wait_for(
        client.collect(task),
        3)

    assert ans1 == ans2

@pytest.mark.asyncio
async def test_task_kwargs(client):
    two = galp.steps.galp_double()
    four = galp.steps.galp_double(two)

    ref = galp.steps.galp_sub(a=four, b=two)
    same = galp.steps.galp_sub(b=two, a=four)

    opposite = galp.steps.galp_sub(a=two, b=four)

    ans = await asyncio.wait_for(
        client.collect(ref, same, opposite),
        3)

    assert tuple(ans) == (2, 2, -2)

@pytest.mark.asyncio
async def test_task_posargs(client):
    two = galp.steps.galp_double()
    four = galp.steps.galp_double(two)

    ref = galp.steps.galp_sub(four, two)
    opposite = galp.steps.galp_sub(two, four)

    ans = await asyncio.wait_for(
        client.collect(ref, opposite),
        3)

    assert tuple(ans) == (2, -2)

@pytest.mark.asyncio
async def test_recollect_intermediate(client):
    """
    Tests doing a collect on a task already run previously as an intermediate
    result.
    """
    two = galp.steps.galp_double()
    four = galp.steps.galp_double(two)

    ans_four = await asyncio.wait_for(
        client.collect(four),
        3)

    ans_two = await asyncio.wait_for(
        client.collect(two),
        3)

    assert tuple(ans_four), tuple(ans_two) == ( (4,), (2,) )

@pytest.mark.asyncio
async def test_stepwise_collect(client):
    """
    Tests running tasks in several incremental collect calls

    (Reverse of recollect_intermediate)
    """
    two = galp.steps.galp_double()
    four = galp.steps.galp_double(two)

    ans_two = await asyncio.wait_for(
        client.collect(two),
        3)

    ans_four = await asyncio.wait_for(
        client.collect(four),
        3)

    assert tuple(ans_four), tuple(ans_two) == ( (4,), (2,) )

async def assert_cache(clients, task=galp.steps.galp_hello()):
    """
    Test worker-side cache.
    """
    client1, client2 = clients

    ans1 = await asyncio.wait_for(client1.collect(task), 3)

    ans2 = await asyncio.wait_for(client2.collect(task), 3)

    assert ans1 == ans2 == [task.step.function()]

    assert client1.submitted_count[task.name] == 1
    assert client2.submitted_count[task.name] == 1

    assert client1.run_count[task.name] == 1
    assert client2.run_count[task.name] == 0

@pytest.mark.asyncio
async def test_mem_cache(client_pair):
    """
    Test worker-side in-memory cache
    """
    await assert_cache(client_pair)

@pytest.mark.asyncio
async def test_fs_cache(disjoined_client_pair):
    """
    Test worker-side fs cache
    """
    await assert_cache(disjoined_client_pair)

@pytest.mark.asyncio
async def test_plugin(client):
    """
    Test running a task defined in a plug-in
    """
    task = gts.plugin_hello()

    ans = await asyncio.wait_for(client.collect(task), 3)

    assert ans == ['6*7']

@pytest.mark.asyncio
async def test_alt_decorator_syntax(client):
    """
    Test declaring a task with decorator called instead of applied.
    """
    task = gts.alt_decorator_syntax()

    ans = await asyncio.wait_for(client.collect(task), 3)

    assert ans == ['alt']

@pytest.mark.asyncio
async def test_vtags(client):
    """
    Exercises version tags
    """
    # Only tagged1 is legit
    tasks = [
        gts.tagged1(),
        gts.tagged2(),
        gts.untagged(),
    ]

    assert len(set(task.name for task in tasks)) == 3

    ans = await asyncio.wait_for(client.collect(tasks[0]), 3)

    assert ans == ['tagged']

@pytest.mark.asyncio
async def test_inline(client):
    """Test using non-task arguments"""

    task = galp.steps.galp_sub(13, 2)
    task2 = galp.steps.galp_sub(b=2, a=13)

    ans = await asyncio.wait_for(
        client.collect(task, task2),
        3)

    assert ans == [11, 11]

@pytest.mark.asyncio
async def test_profiling(client):
    """Tests the integrated python profiler"""
    task = gts.profile_me(27)
    ans = await asyncio.wait_for(client.collect(task), 3)
    assert ans == [196418]

    path = '/tmp/galp_prof/' + task.name.hex() + '.profile'

    assert os.path.isfile(path)

    stats = pstats.Stats(path)
    stats.print_stats()

@pytest.mark.asyncio
async def test_npserializer(client):
    """Tests selecting a different serializer based on type hints"""
    task = gts.arange(10)

    ans = (await asyncio.wait_for(client.collect(task), 3))[0]

    assert isinstance(ans, np.ndarray)
    np.testing.assert_array_equal(ans, np.arange(10))

@pytest.mark.asyncio
async def test_npargserializer(disjoined_client_pair):
    """Tests selecting a different serializer based on type hints, for an input
    task.

    We use a pair of workers to force serialization between tasks"""
    client1, client2 = disjoined_client_pair

    task_in = gts.arange(10)
    task_out = gts.npsum(task_in)

    ans1 = (await asyncio.wait_for(client1.collect(task_in), 3))

    ans2 = (await asyncio.wait_for(client2.collect(task_out), 3))

    assert ans2 == [45]

@pytest.mark.asyncio
async def test_sync_client(async_sync_client_pair):
    """
    Tests the sync client's get functionnality
    """
    aclient, sclient = async_sync_client_pair
    res = galp.steps.galp_hello()

    async_ans = await asyncio.wait_for(aclient.collect(res), 3)

    sync_ans = sclient.get_native(res.handle)

    assert async_ans == [sync_ans] == [42]

@pytest.mark.asyncio
async def test_serialize_df(client):
    """
    Tests basic transfer of dataframe or tables
    """
    task = gts.some_table()

    the_table = task.step.function()

    ans = await asyncio.wait_for(client.collect(task), 3)

    assert ans[0].num_columns == 2
    assert ans[0].num_rows == 3
    assert ans[0] == the_table

@pytest.mark.asyncio
async def test_tuple(client):
    """
    Test tasks yielding a splittable handle.
    """
    task = gts.some_tuple()

    task_a, task_b = task

    task2 = gts.npsum(task_a)

    ans2 = await asyncio.wait_for(client.collect(task2), 3)
    ans_b = await asyncio.wait_for(client.collect(task_b), 3)
    ans_a = await asyncio.wait_for(client.collect(task_a), 3)

    gt_a, gt_b = task.step.function()

    np.testing.assert_array_equal(ans_a[0], gt_a)
    assert gt_b == ans_b[0]

    assert ans2[0] == sum(ans_a[0])

@pytest.mark.asyncio
async def test_collect_tuple(client):
    """
    Test collecting a composite resource
    """
    task = gts.native_tuple()

    ans = await asyncio.wait_for(client.collect(task), 3)

    assert ans[0] == task.step.function()

@pytest.mark.asyncio
async def test_cache_tuple(client_pair):
    """
    Test caching behavior for composite resources.
    """
    await assert_cache(client_pair, gts.native_tuple())

@pytest.mark.asyncio
async def test_step_error(client):
    """
    Test running a task containing a bug
    """
    with pytest.raises(galp.TaskFailedError):
       await asyncio.wait_for(client.collect(gts.raises_error()), 3)

async def test_suicide(client):
    """
    Test running a task triggering a signal
    """
    with pytest.raises(galp.TaskFailedError):
       await asyncio.wait_for(client.collect(gts.suicide(signal.SIGKILL)), 3)

@pytest.mark.asyncio
async def test_step_error_multiple(client):
    """
    Test running a multiple task containing a bug
    """
    with pytest.raises(galp.TaskFailedError):
       await asyncio.wait_for(client.collect(*gts.raises_error_multiple()), 3)

@pytest.mark.asyncio
async def test_missing_step_error(client):
    """
    Test running a unexisting step
    """

    # Define a step locally that is invisible to the worker
    local_export = galp.StepSet()
    @local_export.step
    def missing():
        pass

    with pytest.raises(galp.TaskFailedError):
       await asyncio.wait_for(client.collect(missing()), 3)

@pytest.mark.asyncio
async def test_array_like(client):
    """Test numpy serialization for non-numpy array-like types"""
    ans = await asyncio.wait_for(client.collect(gts.some_numerical_list()), 3)

    np.testing.assert_array_equal(
        ans[0],
        np.array(gts.some_numerical_list().step.function())
        )

@pytest.mark.asyncio
async def test_light_syntax(client):
    task = gts.light_syntax()

    ans = await asyncio.wait_for(client.collect(*task), 3)

    assert tuple(ans) == task.step.function()

@pytest.mark.asyncio
async def test_parallel_tasks(client_pool):
    pre_task = galp.steps.galp_hello()

    # Run a first task to warm the pool
    # FIXME: this is a hack, not a reliable way to ensure workers are online
    pre_ans = await asyncio.wait_for(client_pool.collect(pre_task), 5)

    tasks = [
        gts.sleeps(1, i)
        for i in range(10)
        ]

    ans = await asyncio.wait_for(client_pool.collect(*tasks), 2)

    assert set(ans) == set(range(10))

@pytest.mark.asyncio
async def test_variadic(client):
    args = [1, 2, 3]

    task = gts.sum_variadic(*args)

    ans, = await asyncio.wait_for(client.collect(task), 3)

    assert ans == sum(args)

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
    task_ok = gts.plugin_hello()

    # Ensured the first task fails before we run the second
    with pytest.raises(galp.TaskFailedError):
        ans_fail, = await asyncio.wait_for(client.collect(task_fail), 3)

    ans_fail, ans_ok = await asyncio.wait_for(
        client.collect(task_fail, task_ok, return_exceptions=True),
        3)

    assert type(ans_fail) is galp.TaskFailedError
    assert ans_ok == task_ok.step.function()

async def test_cache_corruption(poisoned_cache, client):
    """
    Raise a TaskFailed if we cannot deserialize in client
    """
    task = poisoned_cache

    with pytest.raises(galp.TaskFailedError):
        ans, = await asyncio.wait_for(
            client.collect(task),
            3)

async def test_remote_cache_corruption(poisoned_cache, client):
    """
    Raise a TaskFailed if we cannot deserialize in worker
    """
    task = poisoned_cache

    task2 = gts.npsum(task)

    with pytest.raises(galp.TaskFailedError):
        ans, = await asyncio.wait_for(
            client.collect(task2),
            3)

    # Test that the worker recovered
    task3 = gts.plugin_hello()
    ans, = await asyncio.wait_for(
        client.collect(task3),
        3)

    assert ans == task3.step.function()

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
    assert all(type(fail) == galp.TaskFailedError for fail in fails)

    ok, = await asyncio.wait_for(
        client.collect(ok_task, return_exceptions=True),
        3)

    assert ok == 1
