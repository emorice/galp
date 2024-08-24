"""
Unit tests for store
"""
from collections import defaultdict
from galp.task_types import step,TaskSerializer
from galp.store import Store

@step
def _scatter1(_arg):
    pass

@step
def _scatter2(_arg):
    pass

@step
def _gather(_arg1, _arg2):
    pass

def test_task_diamond():
    """
    Store does not store twice diamond dependencies
    """
    log = defaultdict(list)
    class _MockCache:
        def __setitem__(self, key, value):
            log[key].append(value)
        def __contains__(self, key):
            return key in log

    store = Store(None, TaskSerializer)
    store.serialcache = _MockCache()

    dep = 'some_literal'
    task = _gather(_scatter1(dep), _scatter2(dep))

    store.put_task(task)

    assert len(log) == 6 # 4 tasks + 2 * 1 literal (children+data)
    for _k, val in log.items():
        assert len(val) == 1
