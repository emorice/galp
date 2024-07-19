"""
High-level tests for galp
"""

import os
import asyncio
import pstats
import time

import numpy as np
from async_timeout import timeout

import galp.steps
import galp.client
import tests.steps as gts

# pylint: disable=redefined-outer-name
# pylint: disable=no-member

# Tests
# =====

async def test_client(client):
    """Test simple functionnalities of client"""
    task = galp.steps.galp_hello()

    ans = await asyncio.wait_for(
        client.collect(task),
        3)

    assert ans == [42,]

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

async def test_task_kwargs(client):
    two = galp.steps.galp_double()
    four = galp.steps.galp_double(two)

    ref = galp.steps.galp_sub(a=four, b=two)
    same = galp.steps.galp_sub(b=two, a=four)

    opposite = galp.steps.galp_sub(a=two, b=four)

    ans = await asyncio.wait_for(
        client.collect(ref, same, opposite),
        4)

    assert tuple(ans) == (2, 2, -2)

async def test_task_posargs(client):
    two = galp.steps.galp_double()
    four = galp.steps.galp_double(two)

    ref = galp.steps.galp_sub(four, two)
    opposite = galp.steps.galp_sub(two, four)

    ans = await asyncio.wait_for(
        client.collect(ref, opposite),
        4)

    assert tuple(ans) == (2, -2)

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

async def assert_cache(clients, task=galp.steps.galp_hello(),
        expected=galp.steps.galp_hello.function()):
    """
    Test worker-side cache.
    """
    client1, client2 = clients

    ans1 = await asyncio.wait_for(client1.collect(task), 3)

    ans2 = await asyncio.wait_for(client2.collect(task), 3)

    assert ans1 == ans2 == [expected]

    assert client1.submitted_count[task.name] == 1
    # Should be 0 since STAT commands were introduced
    assert client2.submitted_count[task.name] == 0

async def test_mem_cache(client_pair):
    """
    Test worker-side in-memory cache
    """
    await assert_cache(client_pair)

async def test_fs_cache(disjoined_client_pair):
    """
    Test worker-side fs cache
    """
    await assert_cache(disjoined_client_pair)

async def test_plugin(client):
    """
    Test running a task defined in a plug-in
    """
    task = gts.hello()

    ans = await asyncio.wait_for(client.collect(task), 3)

    assert ans == ['6*7']

async def test_alt_decorator_syntax(client):
    """
    Test declaring a task with decorator called instead of applied.
    """
    task = gts.alt_decorator_syntax()

    ans = await asyncio.wait_for(client.collect(task), 3)

    assert ans == ['alt']

async def test_vtags(client):
    """
    Exercises version tags
    """
    # Only tag_me is legit
    tasks = [
        gts.tag_me(),
        gts.tagged2(),
        gts.untagged(),
    ]

    assert len(set(task.name for task in tasks)) == 3

    ans = await asyncio.wait_for(client.collect(tasks[0]), 3)

    assert ans == ['tagged']

async def test_inline(client):
    """Test using non-task arguments"""

    task = galp.steps.galp_sub(13, 2)
    task2 = galp.steps.galp_sub(b=2, a=13)

    ans = await asyncio.wait_for(
        client.collect(task, task2),
        3)

    assert ans == [11, 11]

async def test_profiling(client):
    """Tests the integrated python profiler"""
    task = gts.profile_me(27)
    ans = await asyncio.wait_for(client.collect(task), 3)
    assert ans == [196418]

    path = '/tmp/galp_prof/' + task.name.hex() + '.profile'

    assert os.path.isfile(path)

    stats = pstats.Stats(path)
    stats.print_stats()

async def test_npserializer(client):
    """Tests fetching a non-standard type"""
    task = gts.arange(10)

    ans = (await asyncio.wait_for(client.collect(task), 3))[0]

    assert isinstance(ans, np.ndarray)
    np.testing.assert_array_equal(ans, np.arange(10))

async def test_npargserializer(disjoined_client_pair):
    """Tests passing a non-standard type between tasks.

    We use a pair of workers to force serialization between tasks"""
    client1, client2 = disjoined_client_pair

    task_in = gts.arange(10)
    task_out = gts.npsum(task_in)

    ans1 = (await asyncio.wait_for(client1.collect(task_in), 4))

    ans2 = (await asyncio.wait_for(client2.collect(task_out), 4))

    assert ans2 == [45]

async def test_serialize_df(client):
    """
    Tests basic transfer of dataframe or tables
    """
    task = gts.some_table()

    the_table = gts.some_table.function()

    ans = await asyncio.wait_for(client.collect(task), 3)

    assert ans[0].num_columns == 2
    assert ans[0].num_rows == 3
    assert ans[0] == the_table

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

    gt_a, gt_b = gts.some_tuple.function()

    np.testing.assert_array_equal(ans_a[0], gt_a)
    assert gt_b == ans_b[0]

    assert ans2[0] == sum(ans_a[0])

async def test_collect_tuple(client):
    """
    Test collecting a composite resource
    """
    task = gts.native_tuple()

    ans = await asyncio.wait_for(client.collect(task), 3)

    assert ans[0] == gts.native_tuple.function()

async def test_cache_tuple(client_pair):
    """
    Test caching behavior for composite resources.
    """
    await assert_cache(client_pair, gts.native_tuple(),
            gts.native_tuple.function())

async def test_light_syntax(client):
    task = gts.light_syntax()

    ans = await asyncio.wait_for(client.collect(*task), 6)

    assert tuple(ans) == gts.light_syntax.function()

async def test_parallel_tasks(client_pool):
    """
    Runs several long tasks in parallel in a big pool
    """
    all_tasks = [
        gts.sleeps(1, i)
        for i in range(20)
        ]

    pre_tasks, tasks = all_tasks[:10], all_tasks[10:]

    # Run a first batch of tasks to start the pool. The system spawns workers on
    # demand, so the first set of tasks will have quite unpredictable completion
    # times
    time1 = time.time()
    pre_ans = await asyncio.wait_for(client_pool.collect(*pre_tasks), 30)
    dtime1 = time.time() - time1
    assert set(pre_ans) == set(range(10))

    # Run the actual batch and check that all ten run in not much more that the
    # time for one
    time2 = time.time()
    ans = await asyncio.wait_for(client_pool.collect(*tasks), 5)
    dtime2 = time.time() - time2
    assert set(ans) == set(range(10, 20))

    print(f'Warmup: {dtime1:.3f} s')
    print(f'Run   : {dtime2:.3f} s')

async def test_variadic(client):
    """
    Run a task accepting variadic positional arguments
    """
    args = [1, 2, 3]

    task = gts.sum_variadic(*args)

    ans, = await asyncio.wait_for(client.collect(task), 3)

    assert ans == sum(args)

async def test_variadic_dict(client):
    """
    Like variadic for keyword arguments
    """
    kwargs = {'a': 1, 'b': 2, 'c': 3}

    task = gts.sum_variadic_dict(**kwargs)

    ans, = await asyncio.wait_for(client.collect(task), 3)

    assert ans == sum(kwargs.values())

async def test_complex_inline(client):
    """
    Sends a non-native object as inline argument
    """

    async with timeout(3):
        ans = await client.run(
            gts.npsum( np.arange(10) * 1. )
            )
        assert np.allclose(ans, 45.)

async def test_structured_inputs(client):
    """
    Sends list or dicts of tasks to steps
    """
    in1, in2 = gts.identity(3), gts.identity(4)
    in_list = [in1, in2]

    async with timeout(6):
        ans = await client.run(gts.npsum(in_list))
        ans_dict = await client.run(gts.sum_dict({
            'first': in1,
            'second': in2
            }))

    assert ans == 7
    assert ans_dict == 7

async def test_gather_structured(client):
    """
    Runs direclty a list or dict of tasks
    """
    task1, task2 = gts.identity(3), gts.identity(4)
    struct = {'t1': task1, 't2': task2}

    async with timeout(4):
        ans = await client.run(struct)

    assert isinstance(ans, dict)
    assert set(ans.keys()) == set(('t1', 't2'))
    assert ans['t1'] == 3
    assert ans['t2'] == 4

async def test_auto_call(client):
    """
    When steps are given to the client, call them to make them into tasks
    """
    step = gts.hello # not hello()

    async with timeout(3):
        ans = await client.run(step)

    assert ans == step.function()

async def test_collect_empty(client):
    """
    Handle it gracefully if we collect a constant structure
    """
    async with timeout(3):
        ans = await client.run(gts.empty)
    assert len(ans) == 0

async def test_structured_autocalls(client):
    """
    Run a structure with some members being Steps to convert
    """
    step = gts.hello # not hello()

    struct = {'msg': step}

    async with timeout(3):
        ans = await client.run(struct)

    assert ans['msg'] == step.function()
