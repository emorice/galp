"""
Command line and file-oriented features.

These bridge the gap with tools like make.
"""

import subprocess

from async_timeout import timeout

import galp.tests.steps as gts

def test_cli_run(galp_set_one):
    """
    Start a client on the command line and run a task
    """
    endpoint, _ = galp_set_one

    command = f'python3 -m galp.client -e {endpoint} galp.tests.steps hello'

    out = subprocess.getoutput(command)

    assert out == gts.hello.function()

def test_cli_run_noprint(galp_set_one):
    """
    Client on cli without echoing
    """
    endpoint, _ = galp_set_one

    command = f'python3 -m galp.client -q -e {endpoint} galp.tests.steps hello'

    out = subprocess.getoutput(command)

    assert out == ''

async def test_path(client):
    """
    Store a resource to a provided path
    """

    upstream = gts.write_file('42')
    downstream = gts.read_file(upstream)

    async with timeout(3):
        res = await client.run(downstream)
        assert res == '42'
