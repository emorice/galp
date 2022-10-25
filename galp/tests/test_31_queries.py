"""
Query system
"""
import pytest

import galp
import galp.tests.steps as gts

@pytest.mark.xfail
def test_query_status(tmpdir):
    """
    Collect task arguments and there statuses
    """

    graph = [ gts.query.do_nothing(1), gts.query.do_nothing(2) ]

    ans = galp.run(galp.Query(graph, {'*': {'args': '0', 'status': True}}),
            store=tmpdir)

    assert ans == ...
