"""
Run wdl scripts progrmatically inside galp
"""

import os
from functools import partial

import pytest

import galp
import wdl_galp

# pylint: disable=redefined-outer-name

@pytest.fixture
def run(tmpdir):
    """
    Bind test parameters for galp.run
    """
    return partial(galp.run, store=tmpdir, steps=['wdl_galp'],
            log_level='info')

@pytest.fixture
def local_file():
    """
    Resolve local test assets
    """
    self_dir = os.path.dirname(os.path.realpath(__file__))
    def _local_file(name):
        return os.path.join(self_dir, name)

    return _local_file

def test_hello(run, local_file):
    """
    Run the hello world wdl adapted from the specs

    """
    wdl_uri = local_file('hello.wdl')
    infile = local_file('hello.txt')

    task = wdl_galp.run(wdl_uri, infile=infile, pattern="^[a-z]+$")

    assert run(task) == {
          "wf.matches": (
            "hello",
            "world"
            )
        }

def test_missing_arg(run, local_file):
    """
    Run the hello world wdl with a missing arg, resulting in a fail.

    """
    wdl_uri = local_file('hello.wdl')
    infile = local_file('hello.txt')

    with pytest.raises(galp.TaskFailedError):
        run(wdl_galp.run(wdl_uri, infile=infile)) # No "pattern"

def test_two_parts(run, local_file):
    """
    Run two workflows head to tail
    """

    up_wdl = local_file('upstream.wdl')
    down_wdl = local_file('downstream.wdl')

    up_task = wdl_galp.run(up_wdl)
    down_task = wdl_galp.run(down_wdl, **{
        'downstream.in_file': up_task['wf.upstream.out_file']
        })

    assert run(down_task) ==  {'wf.downstream.n_lines': 3}

def test_rel_path(run, local_file):
    """
    Run a wdl workflow with a relative input
    """
    wdl_uri = local_file('hello.wdl')
    infile = os.path.relpath(local_file('hello.txt'))

    task = wdl_galp.run(wdl_uri, infile=infile, pattern="^[a-z]+$")

    assert run(task) == {
          "wf.matches": (
            "hello",
            "world"
            )
        }
