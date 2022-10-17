"""
Command line and file-oriented features.

These bridge the gap with tools like make.
"""

import subprocess
from subprocess import check_output

from async_timeout import timeout

import galp
import galp.tests.steps as gts

def run(command):
    """
    Run a subproces and returns its stdout, rstrip'ed
    """
    return subprocess.check_output(
            command, shell=True, text=True, timeout=3
            ).rstrip()

def test_cli_run(galp_set_one):
    """
    Start a client on the command line and run a task
    """
    endpoint, _ = galp_set_one

    out = run(f'python3 -m galp.client -e {endpoint} galp.tests.steps hello')

    assert out == gts.hello.function()

def test_cli_run_noprint(galp_set_one):
    """
    Client on cli without echoing
    """
    endpoint, _ = galp_set_one

    out = run(f'python3 -m galp.client -q -e {endpoint} galp.tests.steps hello')

    assert out == ''

def test_cli_run_empty(galp_set_one):
    """
    Start a client on the command line and run a task
    """
    endpoint, _ = galp_set_one

    out = run(f'python3 -m galp.client -e {endpoint} galp.tests.steps empty --log-level=info')

    assert out == '()'

def test_cli_jobs(tmpdir):
    """
    Start a pool from cli
    """
    out = run(f'python3 -m galp.client -s {tmpdir} -j 2 galp.steps galp_hello --log-level=info')

    assert out == str(galp.steps.galp_hello.function())

async def test_path(client):
    """
    Store a resource to a provided path
    """

    upstream = gts.files.write_file('42')
    middle = gts.files.copy_file(upstream)
    downstream = gts.files.read_file(middle)

    assert b'_galp' not in upstream.to_dict()['kwarg_names']

    async with timeout(3):
        res = await client.run(downstream)
        assert res == '42'

async def test_inject_path(client):
    """
    Simultaneous use of injection and path providers
    """

    downstream = gts.files.read_file(
            gts.files.injected_copier
            )

    async with timeout(3):
        res = await client.run(downstream)
        assert res == 'wizard'
