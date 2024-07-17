"""
Keep track of the task inside which we're running, to provide access to e.g. the
task name from within a task.
"""

import os
import logging
from dataclasses import dataclass
from contextvars import ContextVar
from contextlib import contextmanager

@dataclass
class PathState:
    """
    State keeping to generate non-conflicting paths inside a given step
    """
    dirpath: str
    task: str
    fileno: int = 0

pathmaker: ContextVar[PathState] = ContextVar('galp.context.pathmaker')

def new_path() -> str:
    """
    Returns a new unique path
    """
    try:
        cur_pathmaker = pathmaker.get()
    except LookupError:
        logging.error('Function `new_path` can only be called from within a '
            + 'running galp task.')
        raise

    fileno = cur_pathmaker.fileno
    cur_pathmaker.fileno += 1
    return os.path.join(
        cur_pathmaker.dirpath,
        'galp',
        f'{cur_pathmaker.task}_{fileno}'
        )

@contextmanager
def set_path(dirpath: str, task: str):
    """
    Initialize path generator for a task

    Args:
        dirpath: path to directory that will contain all files
        task: unique string per task
    """
    token = pathmaker.set(PathState(dirpath, task))
    yield
    pathmaker.reset(token)
