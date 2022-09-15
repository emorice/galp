"""
Globally nuke wdl signal handlers

In the long run we will absorb this logic
"""

from contextlib import contextmanager

import WDL._util

@contextmanager
def _noop(*_, **_k):
    """
    No-op contextmanager
    """
    yield lambda: False

WDL._util.TerminationSignalFlag = _noop
