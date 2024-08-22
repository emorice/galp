"""
Task types, aka graph construction
"""

import pytest

from galp.task_types import step, load_step_by_key, StepLoadError

@step
def _not_scatter():
    pass

@step(scatter=2)
def _small_scatter():
    pass

def test_bad_unpack():
    """
    Unpacking a non-scatter task raises a sensible error

    (and one consistent with python unpack errors)
    """
    # Positive control
    _a, _b = _small_scatter()

    with pytest.raises(TypeError) as exc:
        _a, _b = _not_scatter()
    assert 'cannot unpack' in str(exc)

    with pytest.raises(ValueError) as exc:
        _a, _b, _c = _small_scatter()
    assert 'not enough values' in str(exc)

    with pytest.raises(IndexError) as exc:
        _ = _small_scatter()[2]
    assert 'scatter task index out of range' in str(exc)

@step
def _doc():
    """
    Look I have some documentation
    """

def test_doc():
    """Step preserve docstring"""
    assert 'Look I have some documentation' in _doc.__doc__

def test_bad_import(caplog):
    """Helpful error on failed import"""
    with pytest.raises(StepLoadError):
        load_step_by_key('tests.unit.unimportable::some_step')
    assert 'i_dont_exist' in caplog.text
