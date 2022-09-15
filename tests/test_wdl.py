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
