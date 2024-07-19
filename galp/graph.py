"""
General routines to build and operate on graphs of tasks.
"""
import inspect
import warnings
import functools
import logging

from importlib import import_module
from typing import Any, Callable

import galp.task_types as gtt
from galp.default_resources import get_resources
from galp.task_types import (StepType, TaskNode, CoreTaskDef, QueryTaskDef)

MODULE_SEPARATOR: str = '::'

def raise_if_bad_signature(step: 'Step', args, kwargs) -> None:
    """
    Check arguments number and names so that we can fail when creating a step
    and not only when we run it
    """
    try:
        inspect.signature(step.function).bind(*args, **kwargs)
    except TypeError as exc:
        raise TypeError(
                f'{step.function.__qualname__}() {str(exc)}'
                ) from exc

def make_core_task(step: 'Step', args: list[Any], kwargs: dict[str, Any],
                   resources: gtt.ResourceClaim,
                   vtag: str | None = None, items: int | None = None,
                   ) -> TaskNode:
    """
    Build a core task from a function call
    """
    # Before anything else, type check the call. This ensures we don't wait
    # until actually trying to run the task to realize we're missing
    # arguments.
    raise_if_bad_signature(step, args, kwargs)

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
            'step': step.key,
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

    def __init__(self, scope: 'Block', function: Callable, is_view: bool = False, **task_options):
        self.function = function
        self.key = function.__module__ + MODULE_SEPARATOR + function.__qualname__
        self.task_options = task_options
        self.scope = scope
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

class Block:
    """A collection of steps and bound arguments"""
    def __init__(self, steps=None):
        # Note: if we inherit steps, we don't touch their scope
        self._steps = {} if steps is None else steps

    def step(self, *decorated, **options):
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
            step = Step(
                self,
                function,
                **options,
                )
            self._steps[step.key] = step
            return step

        if decorated:
            return _step_maker(*decorated)
        return _step_maker

    def view(self, *decorated, **options):
        """
        Shortcut to register a view step
        """
        options['is_view'] = True
        return self.step(*decorated, **options)

    def __call__(self, *decorated, **options):
        """
        Shortcut for StepSet.step
        """
        return self.step(*decorated, **options)

    def get(self, key: str) -> Step:
        """Return step by full name"""
        try:
            return self._steps[key]
        except KeyError:
            raise NoSuchStep(key) from None

    @property
    def all_steps(self):
        """
        Dictionary of current steps
        """
        return self._steps

    def __contains__(self, key):
        return key in self._steps

    def __getitem__(self, key):
        return self.get(key)

    def __iadd__(self, other):
        self._steps.update(other._steps)
        # We do not merge the injectables by default
        return self

class StepSet(Block):
    """
    Obsolete alias for PipelineBlock
    """
    def __init__(self, *args, **kwargs):
        warnings.warn(
                'StepSet is now an alias to Block and will be removed in the future',
                FutureWarning, stacklevel=2)
        super().__init__(*args, **kwargs)

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
    """
    module_name, step_name = key.split(MODULE_SEPARATOR)

    try:
        module = import_module(module_name)
    except ModuleNotFoundError:
        logging.error('Error while loading step %s: could not import module %s',
                key, module_name)
        raise

    try:
        return getattr(module, step_name)
    except AttributeError:
        logging.error(
        'Error while loading step %s: could not import find step %s in %s',
                key, step_name, module_name)
        raise
