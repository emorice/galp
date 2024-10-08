"""
Pipeline-writer utils: dashboard, debugging, downloading
"""

from functools import partial

from urllib.request import urlretrieve
from tqdm.auto import tqdm

import galp.commands as cm
import galp.asyn as ga
from galp.result import Result, Ok
from galp.net.core.types import Get
from galp.store import Store
from galp.task_types import (TaskName, TaskRef, Task, CoreTaskDef,
    TaskSerializer, load_step_by_key, step)
from galp.query import collect_task_inputs
from galp.context import new_path


def prepare_task(task_name: TaskName, store_path: str) -> partial:
    """
    Loads input for a task and bind them to the task step.
    """
    store = Store(store_path, TaskSerializer)
    task_def = store.get_task_def(task_name)
    if not isinstance(task_def, CoreTaskDef):
        raise TypeError('Must be a Core task')
    step_obj = load_step_by_key(task_def.step)
    args, kwargs = collect_args(store, TaskRef(task_name), task_def)
    return partial(step_obj.function, *args, **kwargs)

def collect_args(store: Store, task: Task, task_def: CoreTaskDef) -> tuple[list, dict]:
    """
    Re-use client logic to parse the graph and sort out which pieces
    need to be fetched from store
    """
    assert isinstance(task_def, CoreTaskDef)

    ## Define how to fetch missing pieces (direct read from store)
    def _exec(command: cm.Primitive) -> Result:
        """Fulfill commands by reading from stores"""
        if not isinstance(command, cm.Send):
            raise NotImplementedError(command)
        if not isinstance(command.request, Get):
            raise NotImplementedError(command)
        name = command.request.name
        return Ok(store.get_serial(name))

    ## Collect args from local and remote store.
    args, kwargs = ga.run_command(
            collect_task_inputs(task, task_def),
            _exec).unwrap()

    return args, kwargs

def _download_hook(pbar):
    def update(_iter, inc, total):
        if not pbar.total:
            pbar.reset(total)
        pbar.update(inc)
    return update

@step
def download(url: str, progress=True) -> str:
    """
    Retrieve file from url
    """
    path = new_path()
    if progress:
        pbar = tqdm(unit='B', unit_scale=True, delay=0.1, smoothing=0.02,
                    mininterval=.5)
        hook = _download_hook(pbar)
    else:
        hook = None
    try:
        urlretrieve(url, path, reporthook=hook)
    finally:
        if progress:
            pbar.close()
    return path
