"""
General routines to build and operate on graphs of tasks.
"""

from galp.eventnamespace import EventNamespace

def make_handle(task_name):
    return b'H_' + task_name

class Step():
    """Object wrapping a function that can be called as a pipeline step

    'Step' refer to the function itself, 'Task' to the function with its
    arguments: 'add' is a step, 'add(3, 4)' a task. You can run a step twice,
    with different arguments, but that would be two different tasks, and a task
    is only ever run once.
    """
    def __init__(self, function):
        self.function = function
        self.key = bytes(function.__module__ + '::' + function.__qualname__, 'ascii')

class Task():
    """
    Object wrapping a step invocation with its arguments
    """
    def __init__(self, step, args, kwargs):
        self.step = step
        self.args = args
        self.kwargs = kwargs

class StepSet(EventNamespace):
    """A collection of steps"""

    def step(self, function):
        """Decorator to make a function a step.

        This has two effects:
            * it makes the function a delayed function, so that calling it will
              return an object pointing to the actual function, not actually
              execute it. This object is the Task.
            * it register the function and information about it in a event
              namespace, so that it can be called from a key.
              """
        step = Step(function)
        self.on(step.key)(step)
        def _task_maker(*args, **kwargs):
           return Task(step, args, kwargs)

        return _task_maker

    def get(self, key):
        """More memorable shortcut for handler"""
        return self.handler(key)
