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


# Fixtures
# ========

@pytest.fixture
def make_worker(tmp_path):
    """Worker fixture, starts a worker pool in background.

    Returns:
        (endpoint, Popen) tuple
    """
    # todo: safer port picking
    port = itertools.count(48652)
    phandles = []

    def _make(endpoint=None, pool_size=1):
        if endpoint is None:
            endpoint = f"tcp://127.0.0.1:{next(port)}"

        phandle = subprocess.Popen([
            sys.executable,
            '-m', 'galp.pool',
            str(pool_size),
            '-c', 'galp/tests/config.toml',
            #'--debug',
            endpoint, str(tmp_path)
            ])
        phandles.append(phandle)

        return endpoint, phandle

    yield _make

    # If it's a kill test it's already done, later with remote worker we'll rely
    # on messages here
    # Note: terminate should be safe to call no matter how dead the child
    # already is
    for phandle in phandles:
        phandle.terminate()
        phandle.wait()

@pytest.fixture
def make_broker():
    """Broker fixture, starts a broker in background.

    Returns:
        (endpoint, Popen) tuple
    """
    # todo: safer port picking
    port = itertools.count(48652)
    phandles = []

    def _make():
        client_endpoint = f"tcp://127.0.0.1:{next(port)}"
        worker_endpoint = f"tcp://127.0.0.1:{next(port)}"

        phandle = subprocess.Popen([
            sys.executable,
            '-m', 'galp.broker',
            #'--debug',
            client_endpoint,
            worker_endpoint,
            ])
        phandles.append(phandle)

        return client_endpoint, worker_endpoint, phandle

    yield _make

    for phandle in phandles:
        phandle.terminate()
        phandle.wait()

@pytest.fixture
def worker(make_worker):
    return make_worker()

@pytest.fixture
def broker(make_broker):
    return make_broker()

@pytest.fixture
def make_worker_pool(broker, make_worker):
    """
    A pool of n workers
    """
    def _make(n):
        cl_ep, w_ep, _ = broker
        _, phandle = make_worker(w_ep, n)
        return cl_ep, [phandle]

    return _make

@pytest.fixture
def worker_pool(make_worker_pool):
    """
    A pool of 10 workers
    """
    return make_worker_pool(10)

@pytest.fixture
def broker_worker(broker, make_worker_pool):
    """
    A "pool" with a single worker
    """
    return make_worker_pool(1)

@pytest.fixture
def ctx():
    """The ØMQ context"""
    ctx =  zmq.Context()
    yield ctx
    # During testing we do a lot of fancy stuff such as trying to talk to the
    # dead and aborting a lot, so don't panic if there's still stuff in the
    # pipes -> linger.
    logging.warning('Now destroying context...')
    ctx.destroy(linger=1)
    logging.warning('Done')

@pytest.fixture
def async_ctx():
    """The ØMQ context, asyncio flavor"""
    ctx =  zmq.asyncio.Context()
    yield ctx
    # During testing we do a lot of fancy stuff such as trying to talk to the
    # dead and aborting a lot, so don't panic if there's still stuff in the
    # pipes -> linger.
    logging.warning('Now destroying context...')
    ctx.destroy(linger=1)
    logging.warning('Done')

@pytest.fixture
def worker_socket(ctx, make_worker):
    """Dealer socket connected to some worker"""

    socket = ctx.socket(zmq.DEALER)
    socket.bind('tcp://127.0.0.1:*')
    endpoint = socket.getsockopt(zmq.LAST_ENDPOINT)

    endpoint, handle = make_worker(endpoint)

    yield socket, endpoint, handle

    # Closing with linger since we do not know if the test has failed or left
    # pending messages for whatever reason.
    socket.close(linger=1)

@pytest.fixture
def make_async_socket(async_ctx):
    """Factory feature to create several sockets to a set of workers.

    The return factory takes an endpoint as sole argument.

    Note that youbroker_r endpoint must be able to deal with several clients !"""

    """
    Helper to create both sync and async sockets
    """
    sockets = []
    def _make(endpoint):
        socket = async_ctx.socket(zmq.DEALER)
        sockets.append(socket)
        socket.bind(endpoint)
        return socket

    yield _make

    while sockets:
        # Closing with linger since we do not know if the test has failed or left
        # pending messages for whatever reason.
        sockets.pop().close(linger=1)

@pytest.fixture
def async_worker_socket(make_async_socket, worker):
    """Dealer socket connected to some worker, asyncio flavor"""
    endpoint, _ = worker
    yield make_async_socket(endpoint)

@pytest.fixture(params=[b'EXIT', b'ILLEGAL'])
def fatal_order(request):
    """All messages that should make the worker quit"""
    return request.param

@pytest.fixture
def make_client():
    """Factory fixture for client managing its own sockets"""
    def _make(endpoint):
        return galp.client.Client(endpoint)
    return _make

@pytest.fixture
def client(make_client, broker_worker):
    """A client connected to a pool with one worker"""
    endpoint, _ = broker_worker
    return make_client(endpoint)

@pytest.fixture
def client_pool(make_client, worker_pool):
    """A client connected to a pool of 10 workers"""
    endpoint, _ = worker_pool
    return make_client(endpoint)

@pytest.fixture
def client_pair(broker_worker):
    """A pair of clients connected to one worker.

    This is now implemented with two incoming connections to the same broker/worker,
    routing on worker-side should disantangle things.
    """
    endpoint, _ = broker_worker
    c1 = galp.client.Client(endpoint=endpoint)
    c2 = galp.client.Client(endpoint=endpoint)
    return c1, c2

@pytest.fixture
def disjoined_client_pair(make_worker_pool, make_client):
    """A pair of client connected to two different workers"""
    e1, _ = make_worker_pool(1)
    e2, _ = make_worker_pool(1)
    return make_client(e1), make_client(e2)

@pytest.fixture
def async_sync_client_pair(make_worker_pool):
    e1, _ = make_worker_pool(1)
    e2, _ = make_worker_pool(1)
    yield galp.client.Client(endpoint=e1), galp.synclient.SynClient(endpoint=e2)

# Helpers
# =======

def asserted_zmq_recv_multipart(socket):
    selectable = [socket], [], []
    assert zmq.select(*selectable, timeout=4) == selectable
    return socket.recv_multipart()

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

    ans1 = asserted_zmq_recv_multipart(socket)
    assert ans1 == [b'', b'READY']

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

    ans1 = asserted_zmq_recv_multipart(socket)
    assert ans1 == [b'', b'READY']
    ans2 = asserted_zmq_recv_multipart(socket)
    assert ans2 == [b'', b'ILLEGAL']

def test_task(worker_socket):
    socket, *_ = worker_socket

    task = galp.steps.galp_hello()
    step_name = task.step.key

    logging.warning('Calling task %s', step_name)

    handle = galp.graph.Task.gen_name(step_name, [], {}, [])

    socket.send_multipart([b'', b'SUBMIT', step_name, b'\x00'])

    ans = asserted_zmq_recv_multipart(socket)
    assert ans == [b'', b'READY']

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

    ans = asserted_zmq_recv_multipart(worker_socket)
    assert ans == [b'', b'READY']

    ans = asserted_zmq_recv_multipart(worker_socket)
    assert ans == [b'', b'NOTFOUND', bad_handle]

def test_reference(worker_socket):
    """Tests passing the result of a task to an other through handle"""
    worker_socket, *_ = worker_socket

    task1 = galp.steps.galp_double()

    task2 = galp.steps.galp_double(task1)

    worker_socket.send_multipart([b'', b'SUBMIT', task1.step.key, b'\x00'])

    ready = asserted_zmq_recv_multipart(worker_socket)
    assert ready == [b'', b'READY']

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
    sock = async_worker_socket

    await asyncio.wait_for(sock.send_multipart([b'', b'RABBIT']), 3)

    ready = await asyncio.wait_for(sock.recv_multipart(), 3)
    assert ready == [b'', b'READY']

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

@pytest.mark.xfail
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
async def test_signals_busyloop(client, broker_worker, sig):
    """Tests that worker terminate on signal when stuck in busy loop"""
    client_endoint, worker_handles = broker_worker
    task = gts.busy_loop()

    bg = asyncio.create_task(client.collect(task))

    #FIXME: we should wait for the DOING message instead
    await asyncio.sleep(1)

    for worker_handle in worker_handles:
        worker_handle.send_signal(sig)

    worker_handle.wait(timeout=4)

    bg.cancel()
    try:
        await bg
    except asyncio.CancelledError:
        pass

