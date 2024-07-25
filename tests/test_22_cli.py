"""
Command line and file-oriented features.

These bridge the gap with tools like make.
"""

import asyncio
import subprocess
from async_timeout import timeout

import pytest

import galp
import tests.steps as gts

async def run(command):
    """
    Run a subprocess and returns its stdout, rstrip'ed
    """
    proc = await asyncio.create_subprocess_shell(
        command,
        stdout=asyncio.subprocess.PIPE,
        stderr=None)

    async with timeout(3):
        stdout, stderr = await proc.communicate()

    if proc.returncode != 0:
        raise subprocess.CalledProcessError(
                returncode=proc.returncode,
                cmd=command)
    assert stderr is None
    return stdout.decode().rstrip()

async def test_cli_run(galp_set_one):
    """
    Start a client on the command line and run a task
    """
    endpoint, _ = galp_set_one

    out = await run(f'python3 -m galp.client -e {endpoint} tests.steps hello')

    assert out == gts.hello.function()

async def test_cli_run_noprint(galp_set_one):
    """
    Client on cli without echoing
    """
    endpoint, _ = galp_set_one

    out = await run(f'python3 -m galp.client -q -e {endpoint} tests.steps hello')

    assert out == ''

async def test_cli_run_empty(galp_set_one):
    """
    Start a client on the command line and run a task
    """
    endpoint, _ = galp_set_one

    out = await run(f'python3 -m galp.client -e {endpoint} tests.steps empty --log-level=info')

    assert out == '()'

async def test_cli_jobs(tmpdir):
    """
    Start a pool from cli
    """
    out = await run(f'python3 -m galp.client -s {tmpdir} -j 2 galp.steps galp_hello '
        '--log-level=info --pin-workers')

    assert out == str(galp.steps.galp_hello.function())

async def test_cli_keep_going(tmpdir):
    """
    Start a failing job
    """
    with pytest.raises(subprocess.CalledProcessError):
        _ = await run(f'python3 -m galp.client -s {tmpdir} tests.steps suicide')

    out = await run(f'python3 -m galp.client -s {tmpdir} -k tests.steps suicide')
    assert 'failed' in out.lower()

async def test_path(client):
    """
    Store a resource to a provided path
    """

    upstream = gts.files.write_file('42')
    middle = gts.files.copy_file(upstream)
    downstream = gts.files.read_file(middle)

    assert not upstream.task_def.kwargs

    async with timeout(3):
        res = await client.run(downstream)
        assert res == '42'
