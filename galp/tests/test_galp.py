"""
Generic tests for galp
"""

import os
import sys
import subprocess
import logging
import asyncio
import itertools
import signal
import time
import pstats

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
    """Worker fixture, starts a worker in background.

    Returns:
        (endpoint, Popen) tuple
    """
    # todo: safer port picking
    port = itertools.count(48652)
    phandles = []

    def _make():
        endpoint = f"tcp://127.0.0.1:{next(port)}"

        phandle = subprocess.Popen([
            sys.executable,
            '-m', 'galp.worker',
            '-c', 'galp/tests/config.toml',
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
def worker(make_worker):
    return make_worker()

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
def worker_socket(ctx, worker):
    """Dealer socket connected to some worker"""
    endpoint, _ = worker

    socket = ctx.socket(zmq.DEALER)
    socket.connect(endpoint)

    yield socket

    # Closing with linger since we do not know if the test has failed or left
    # pending messages for whatever reason.
    socket.close(linger=1)

@pytest.fixture
def make_async_socket(async_ctx):
    """Factory feature to create several sockets to a set of workers.

    The return factory takes an endpoint as sole argument.

    Note that your endpoint must be able to deal with several clients !"""

    """
    Helper to create both sync and async sockets
    """
    sockets = []
    def _make(endpoint):
        socket = async_ctx.socket(zmq.DEALER)
        sockets.append(socket)
        socket.connect(endpoint)
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
def make_client(make_async_socket):
    def _make(endpoint):
        socket = make_async_socket(endpoint)
        return galp.client.Client(socket)
    return _make

@pytest.fixture
def client(make_client, worker):
    """A client connected to a worker"""
    endpoint, _ = worker
    return make_client(endpoint)

@pytest.fixture
def make_standalone_client():
    """Factory fixture for client managing its own sockets"""
    def _make(endpoint):
        return galp.client.Client(endpoint=endpoint)
    return _make

@pytest.fixture(params=['preconnected', 'standalone'])
def any_client(request, make_client, make_standalone_client, worker):
    """A client connected to a worker, with different creation methods"""
    endpoint, _ = worker
    if request.param == 'preconnected':
        return make_client(endpoint)
    else:
        return make_standalone_client(endpoint)

@pytest.fixture
def client_pair(async_worker_socket):
    """A pair of clients connected to the same socket and worker.

    Obviously unsafe for concurrent use. When we introduce routing, swap this
    implementation to use two sockets.
    """
    s = async_worker_socket
    c1 = galp.client.Client(s)
    c2 = galp.client.Client(s)
    return c1, c2

@pytest.fixture
def disjoined_client_pair(make_worker, make_client):
    """A pair of client connected to two different workers"""
    e1, w1 = make_worker()
    e2, w2 = make_worker()
    return make_client(e1), make_client(e2)

@pytest.fixture
def async_sync_client_pair(make_worker):
    e1, w1 = make_worker()
    e2, w2 = make_worker()
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
    socket.connect(endpoint)
    socket.send(fatal_order)

    assert worker_handle.wait(timeout=4) == 0

    # Note: we only close on normal termination, else we rely on the fixture
    # finalization to set the linger before closing.
    socket.close()

@pytest.mark.parametrize('sig', [signal.SIGINT, signal.SIGTERM])
def test_signals(worker, sig):
    """Test for clean termination on INT and TERM)"""

    endpoint, worker_handle = worker

    assert worker_handle.poll() is None

    # Race condition, process exists before signal handlers do
    # Not a problem in practice, before handlers are set there's no cleanup to
    # do anyway
    # We can plug a 'ready' event from worker later on
    time.sleep(0.5)
    worker_handle.send_signal(sig)

    assert worker_handle.wait(timeout=4) == 0

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

    worker_socket.send_multipart(msg)

    ans = asserted_zmq_recv_multipart(worker_socket)
    assert ans == [b'ILLEGAL']

def test_task(worker_socket):
    task = galp.steps.galp_hello()
    step_name = task.step.key

    logging.warning('Calling task %s', step_name)

    handle = galp.graph.Task.gen_name(step_name, [], {}, [])

    worker_socket.send_multipart([b'SUBMIT', step_name, b'\x00'])

    ans = asserted_zmq_recv_multipart(worker_socket)
    assert ans == [b'DOING', handle]

    ans = asserted_zmq_recv_multipart(worker_socket)
    assert ans == [b'DONE', handle]

    worker_socket.send_multipart([b'GET', handle])

    ans = asserted_zmq_recv_multipart(worker_socket)
    assert ans[:3] == [b'PUT', handle, b'dill']
    assert dill.loads(ans[3]) == 42

def test_notfound(worker_socket):
    """Tests the answer of server when asking to send unexisting resource"""
    bad_handle = b'RABBIT'
    worker_socket.send_multipart([b'GET', bad_handle])

    ans = asserted_zmq_recv_multipart(worker_socket)
    assert ans == [b'NOTFOUND', bad_handle]

def test_reference(worker_socket):
    """Tests passing the result of a task to an other through handle"""

    task1 = galp.steps.galp_double()

    task2 = galp.steps.galp_double(task1)

    worker_socket.send_multipart([b'SUBMIT', task1.step.key, b'\x00'])
    # doing
    doing = asserted_zmq_recv_multipart(worker_socket)
    done = asserted_zmq_recv_multipart(worker_socket)
    assert doing, done == ([b'DOING', task1.name], [b'DONE', task1.name])

    worker_socket.send_multipart([b'SUBMIT', task2.step.key, b'\x00', b'', task1.name])

    doing = asserted_zmq_recv_multipart(worker_socket)
    done = asserted_zmq_recv_multipart(worker_socket)
    assert doing, done == ([b'DOING', task2.name], [b'DONE', task2.name])

    # Let's try async get for a twist !
    worker_socket.send_multipart([b'GET', task2.name])
    worker_socket.send_multipart([b'GET', task1.name])

    # Order of the answers is unspecified
    got_a = asserted_zmq_recv_multipart(worker_socket)
    got_b = asserted_zmq_recv_multipart(worker_socket)

    assert got_a[0] == got_b[0] == b'PUT'
    assert set((got_a[1], got_b[1])) == set((task1.name, task2.name))
    expected = {
        task1.name: 2,
        task2.name: 4
        }
    assert got_a[2] == got_b[2] == b'dill'
    for _, name, proto, res, *children in [got_a, got_b]:
        assert dill.loads(res) == expected[name]

@pytest.mark.asyncio
async def test_async_socket(async_worker_socket):
    sock = async_worker_socket
    await asyncio.wait_for(sock.send(b'RABBIT'), 3)
    ans = await asyncio.wait_for(sock.recv_multipart(), 3)
    assert ans == [b'ILLEGAL']

@pytest.mark.asyncio
async def test_client(any_client):
    """Test simple functionnalities of client"""
    task = galp.steps.galp_hello()

    ans = await asyncio.wait_for(
        any_client.collect(task),
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
