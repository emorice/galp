"""
Galp test steps used to build other steps
"""

import galp

export = galp.Block()

@export
def identity(arg):
    """
    Returns its arg unchanged
    """
    print(arg)
    return arg

@export
def get_cpus():
    """
    Return currently configured number of (openmp) threads
    """
    # pylint: disable=import-outside-toplevel
    import numpy # pylint: disable=unused-import # side effect
    import threadpoolctl # type: ignore[import]
    print(threadpoolctl.threadpool_info())
    return threadpoolctl.threadpool_info()[-1]['num_threads']
