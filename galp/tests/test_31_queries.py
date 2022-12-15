"""
Query system
"""
import pytest

import galp
import galp.tests.steps as gts

def test_query_status(tmpdir):
    """
    Collect task arguments and there statuses
    """

    graph = [ gts.query.do_nothing(1), gts.query.do_nothing(2) ]

    ans = galp.run(galp.Query(graph, {'*': {'args': ['0'], 'done': True}}),
            store=tmpdir)

    assert ans ==  {'*': (
        {'args': {'0': 1}, 'done': False},
        {'args': {'0': 2}, 'done': False}
        )}

def test_query_def(tmpdir):
    """
    Collect task definition
    """

    task = gts.query.do_nothing(1)

    ans = galp.run(galp.Query(task, 'def'), store=tmpdir)

    assert 'step_name' in ans
    assert ans['step_name'] == task.step.key

def test_query_children(tmpdir):
    """
    Collect meta-task children definition
    """

    task = gts.query.do_meta()

    ans = galp.run(galp.Query(task, {'children': {'0': 'def'}}),
        store=tmpdir,
        steps=['galp.tests.steps'])

    assert 'children' in ans
    assert all(c['step_name']  == gts.query.do_nothing(0).step.key
            for _k, c in ans['children'].items())
