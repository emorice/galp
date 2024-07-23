"""
Shared utils for dashboard and debugging
"""

from functools import partial

import galp.commands as cm
import galp.asyn as ga
from galp.result import Result, Ok
from galp.net.core.types import Get
from galp.cache import CacheStack
from galp.task_types import TaskName, TaskRef, Task, CoreTaskDef, TaskSerializer
from galp.graph import load_step_by_key
from galp.query import collect_task_inputs

def prepare_task(task_name: TaskName, store_path: str) -> partial:
    """
    Loads input for a task and bind them to the task step.
    """
    store = CacheStack(store_path, TaskSerializer)
    task_def = store.get_task_def(task_name)
    if not isinstance(task_def, CoreTaskDef):
        raise TypeError('Must be a Core task')
    step = load_step_by_key(task_def.step)
    args, kwargs = collect_args(store, TaskRef(task_name), task_def)
    return partial(step.function, *args, **kwargs)

def collect_args(store: CacheStack, task: Task, task_def: CoreTaskDef) -> tuple[list, dict]:
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
