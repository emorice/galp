"""
General routines to build and operate on graphs of tasks.
"""
import logging
import hashlib

from galp.eventnamespace import EventNamespace

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
        self.name = self.gen_name(
            step.key,
            [ arg.name for arg in args ],
            { k.encode('ascii') : v.name for k, v in kwargs.items() }
            )

    @property
    def dependencies(self):
        """Shorthand for all the tasks this task directly depends on"""
        deps = list(self.args)
        deps.extend(self.kwargs.values())
        return deps

    @staticmethod
    def gen_name(step_name, arg_names, kwarg_names):
        """Create a resource name.

        Args:
            step_name: bytes
            arg_names: list of bytes
            kwarg_names: bytes-keyed dict of bytes.

        Returns:
            digest as bytes.

        We simply mash together the step name, and the args name in order.
        The only difficulty is the order of keyword arguments, so we sort kwargs
        by keyword (in original byte representation).
        All input is sanitized by hexing it, then building a representation from
        it.

        """
        def _san(bts):
            """This constrains the range of values of the resulting bytes"""
            return bts.hex().encode('ascii')

        sortedkw = sorted(list(kwarg_names.items()))

        m = _san(step_name)
        m += b'('
        m += b','.join(
            [ _san(n) for n in arg_names]
            +
            [ _san(kw) + b'=' + _san(n) for kw, n in sortedkw ]
            )
        m += b')'
        logging.warning("Hashed value is %s:", m)

        return hashlib.sha256(m).digest()

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
