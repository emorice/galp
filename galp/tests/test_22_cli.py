"""
Diverse command line interfaces
"""

import subprocess
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
