"""
Globally nuke wdl signal handlers

In the long run we will absorb this logic
"""

import logging
from contextlib import contextmanager

try:
    import WDL._util
except ModuleNotFoundError as exc:
    logging.error('The galp.wdl extension requires the wdl galp extra'
            ' dependencies, which can be for instance installed with:\n\n'
            '    pip install galp[wdl]\n')
    raise

@contextmanager
def _noop(*_, **_k):
    """
    No-op contextmanager
    """
    yield lambda: False

WDL._util.TerminationSignalFlag = _noop
