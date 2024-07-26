"""
General routines to build and operate on graphs of tasks.
"""
import inspect
import functools
import logging

from importlib import import_module
from typing import Any, Callable

import galp.task_types as gtt
from galp.default_resources import get_resources
from galp.task_types import (StepType, TaskNode, CoreTaskDef, QueryTaskDef)

MODULE_SEPARATOR: str = '::'

def raise_if_bad_signature(stp: 'Step', args, kwargs) -> None:
    """
    Check arguments number and names so that we can fail when creating a step
    and not only when we run it
    """
    try:
        inspect.signature(stp.function).bind(*args, **kwargs)
    except TypeError as exc:
        raise TypeError(
                f'{stp.function.__qualname__}() {str(exc)}'
                ) from exc

def make_core_task(stp: 'Step', args: list[Any], kwargs: dict[str, Any],
                   resources: gtt.ResourceClaim,
                   vtag: str | None = None, items: int | None = None,
                   ) -> TaskNode:
    """
    Build a core task from a function call
    """
    # Before anything else, type check the call. This ensures we don't wait
    # until actually trying to run the task to realize we're missing
    # arguments.
    raise_if_bad_signature(stp, args, kwargs)

    # Collect all arguments as nodes
    arg_inputs = []
    kwarg_inputs = {}
    nodes = []
    for arg in args:
        tin, node = gtt.ensure_task_input(arg)
        arg_inputs.append(tin)
        nodes.append(node)
    for key, arg in kwargs.items():
        tin, node = gtt.ensure_task_input(arg)
        kwarg_inputs[key] = tin
        nodes.append(node)

    tdef = {
            'args': arg_inputs,
            'kwargs': kwarg_inputs,
            'step': stp.key,
            'vtags': ([ascii(vtag) ] if vtag is not None else []),
            'scatter':items,
            'resources': resources
            }

    ndef = gtt.make_task_def(CoreTaskDef, tdef)

    return TaskNode(task_def=ndef, dependencies=nodes)

class Step(StepType):
    """Object wrapping a function that can be called as a pipeline step

    'Step' refer to the function itself, 'Task' to the function with its
    arguments: 'add' is a step, 'add(3, 4)' a task. You can run a step twice,
    with different arguments, but that would be two different tasks, and a task
    is only ever run once.

    Args:
        items: int, signal this task eventually returns a collection of items
            objects, and allows to index or iterate over the task to generate
            handle pointing to the individual items.
    """

    def __init__(self, function: Callable, is_view: bool = False, **task_options):
        self.function = function
        self.key = function.__module__ + MODULE_SEPARATOR + function.__qualname__
        self.task_options = task_options
        self.is_view = is_view
        functools.update_wrapper(self, self.function)

    def __call__(self, *args, **kwargs) -> TaskNode:
        """
        Symbolically call the function wrapped by this step, returning a Task
        object representing the eventual result.
        """
        return self.make_task(args, kwargs)

    def make_task(self, args, kwargs) -> TaskNode:
        """
        Create actual Task object from the given args.

        Does not perform the injection
        """
        return make_core_task(self, args, kwargs, get_resources(), **self.task_options)

    def __getitem__(self, index):
        """
        Return a new step representing the eventual result of injecting then
        subscripting this step
        """
        return SubStep(self, index)

    def __get__(self, obj, objtype=None):
        """
        Normally, a function wrapper can be transparently applied to methods too
        through the descriptor protocol, but it's not clear what galp should do
        in that case, so we explicitly error out.

        The special function is defined anyway because code may rely on it to
        detect that the Step object is a function wrapper.
        """

        raise NotImplementedError('Cannot only wrap functions, not methods')

class SubStep(StepType):
    """
    Step representing the indexation of the result of an other step
    """
    def __init__(self, parent, index):
        self.parent = parent
        self.index = index

    def __call__(self, *args, **kwargs):
        """
        Inject and call the parent step, then index the resulting task
        """
        return self.parent(*args, **kwargs)[self.index]

    def make_task(self, args, kwargs):
        """
        Create parent task and subscript it.
        """
        return self.parent.make_task(args, kwargs)[self.index]

class NoSuchStep(ValueError):
    """
    No step with the given name has been registered
    """

def step(*decorated, **options):
    """Decorator to make a function a step.

    This has two effects:
        * it makes the function a delayed function, so that calling it will
          return an object pointing to the actual function, not actually
          execute it. This object is the Task.
        * it register the function and information about it in a event
          namespace, so that it can be called from a key.

    For convenience, the decorator can be applied in two fashions:
    ```
    @step
    def foo():
        pass
    ```
    or
    ```
    @step(param=value, ...)
    def foo():
        pass
    ```
    This is allowed because the decorator checks whether it was applied
    directly to the function, or called with named arguments. In the second
    case, note that arguments must be given by name for the call to be
    unambiguous.

    See Task for the possible options.
    """
    def _step_maker(function):
        return Step(function, **options)

    if decorated:
        return _step_maker(*decorated)
    return _step_maker

def view(*decorated, **options):
    """
    Shortcut to register a view step
    """
    options['is_view'] = True
    return step(*decorated, **options)

def query(subject: Any, query_doc: Any) -> TaskNode:
    """
    Build a Query task node
    """
    subj_node = gtt.ensure_task_node(subject)
    tdef = {'query': query_doc, 'subject': subj_node.name}
    return TaskNode(
            task_def=gtt.make_task_def(
                QueryTaskDef, tdef
                ),
            dependencies=[subj_node]
            )

def load_step_by_key(key: str) -> Step:
    """
    Try to import the module containing a step and access such step

    Raises:
        NoSuchStep: if loading the module or accessing the step fails
    """
    module_name, step_name = key.split(MODULE_SEPARATOR)

    try:
        module = import_module(module_name)
    except ModuleNotFoundError as exc:
        logging.error('Error while loading step %s: could not import module %s',
                key, module_name)
        raise NoSuchStep(key) from exc

    try:
        return getattr(module, step_name)
    except AttributeError as exc:
        logging.error(
        'Error while loading step %s: could not import find step %s in %s',
                key, step_name, module_name)
        raise NoSuchStep(key) from exc
