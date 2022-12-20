"""
Query system
"""
import galp
import galp.tests.steps as gts

from galp.graph import TaskReference

def test_query_status(tmpdir):
    """
    Collect task arguments and there statuses
    """

    graph = [ gts.query.do_nothing(1), gts.query.do_nothing(2) ]

    ans = galp.run(galp.Query(graph, {'*': {'$args': {'0': True}, '$done': True}}),
            store=tmpdir)

    assert ans ==  {'*': [
        {'$args': {'0': 1}, '$done': False},
        {'$args': {'0': 2}, '$done': False}
        ]}

def test_query_def(tmpdir):
    """
    Collect task definition
    """

    task = gts.query.do_nothing(1)

    ans = galp.run(galp.Query(task, '$def'), store=tmpdir)

    assert 'step_name' in ans
    assert ans['step_name'] == task.step.key

def test_query_children(tmpdir):
    """
    Collect meta-task children definition
    """

    task = gts.query.do_meta()

    ans = galp.run(galp.Query(task, {'$children': {'0': '$def'}}),
        store=tmpdir,
        steps=['galp.tests.steps'])

    assert '$children' in ans
    assert all(c['step_name']  == gts.query.do_nothing(0).step.key
            for _k, c in ans['$children'].items())

def test_query_all_children(tmpdir):
    """
    Collect meta-task children definition, iterative version
    """

    task = gts.query.do_meta()

    ans = galp.run(galp.Query(task, {'$children': {'*': '$def'}}),
        store=tmpdir,
        steps=['galp.tests.steps'])

    assert '$children' in ans
    assert sum(
            c['step_name']  == gts.query.do_nothing(0).step.key
            for _k, c in ans['$children'].items()
            ) == 2

def test_query_base(tmpdir):
    """
    Test non-recursive task collection
    """
    graph = [ gts.query.do_nothing(1), gts.query.do_nothing(2) ]

    ans = galp.run(galp.Query(graph, '$base'),
            store=tmpdir, steps=['galp.tests.steps'])

    assert len(ans) == 2
    assert all(isinstance(t, TaskReference) for t in ans)

def test_query_index(tmpdir):
    """
    Test indexing directly inside task result
    """
    graph = {'x': gts.query.do_nothing(1), 'y': gts.query.do_nothing(2)}

    ans = galp.run(galp.Query(graph, {'x': '$done'}),
            store=tmpdir, steps=['galp.tests.steps'])

    assert ans == {'x': False}

def test_query_num_index(tmpdir):
    """
    Test indexing directly inside task result, with numeric indexing
    """
    graph = [ gts.query.do_nothing(1), gts.query.do_nothing(2) ]

    ans = galp.run(galp.Query(graph, {'1': '$done'}),
            store=tmpdir, steps=['galp.tests.steps'])

    assert ans == {'1': False}
