"""
Steps only used for testing, and loaded through the plugin system.
"""
import os
import sys
import time
import logging
import signal

import psutil
import numpy as np
import pyarrow as pa # type: ignore[import]

import galp
from galp import step

from . import utils, dashboard, files, query
from .utils import identity

@step
def hello():
    """
    Takes no arguments, returns a constant, simplest step
    """
    return '6*7'

@step()
def alt_decorator_syntax():
    """
    Step using a call to `step()` in the decorator
    """
    return 'alt'

def tag_me():
    """
    Input for steps with tags
    """
    return 'tagged'

untagged = step(tag_me)
tagged2 = step(vtag=1)(tag_me)

# Only this one can be called because it is re-bound to the true name. The other
# two can only be used to test naming
tag_me = step(vtag=0)(tag_me)


def naive_fib(n): # pylint: disable=invalid-name
    """
    Recursive fibonacci calculation, useful to put some load on the interpreter.
    """
    if n in (1, 2):
        return 1
    return naive_fib(n-1) + naive_fib(n-2)

@step
def profile_me(n): # pylint: disable=invalid-name
    """
    Wrapper around `naive_fib`
    """
    return naive_fib(n)

@step
def arange(n): # pylint: disable=invalid-name
    """Numpy return type"""
    return np.arange(n)

@step
def npsum(vect):
    """
    Numpy input but generic return type
    """
    return float(np.sum(vect))

@step
def some_table():
    """
    Pyarrow return type
    """
    return  pa.table({
        'x': ['abc', 'def', 'gh'],
        'y': [1.0, -0.0, 7]
        })

@step(items=2)
def some_tuple():
    """
    Returns two resources that can be addressed separately
    """
    return (
        np.arange(10),
        10
    )

@step(items=2)
def native_tuple():
    """
    Returns two resources that can be addressed separately
    """
    return (
        'fluff',
        14
    )

@step
def raises_error():
    """
    Never completes, raises a divide-by-zero error
    """
    return 1 / 0

@step
def divide(num, den):
    """
    Division, errors when den is 0
    """
    return num / den

@step(items=3)
def light_syntax():
    """
    Uses directly the StepSet as a decorator
    """
    return 5, ("a", "b"), {'x': 7.}

@step(items=2)
def raises_error_multiple():
    """
    Like raises_error, but returns an iterable handle
    """
    return True, 1 / 0

@step
def sleeps(secs, some_arg):
    """
    Sleeps for the given time and return the argument unchanged
    """
    print(f'Sleeping for {secs}s', flush=True)
    time.sleep(secs)
    print(f'Slept for {secs}s', flush=True)
    return some_arg

@step
def sum_variadic(*args):
    """
    Step that takes variadic positional args
    """
    return sum(args)

@step
def sum_variadic_dict(**kwargs):
    """
    Step that takes variadic keyword args
    """
    return sum(kwargs.values())

@step
def busy_loop():
    """
    Infinite pure python loop
    """
    while True:
        pass

@step
def suicide(sig=signal.SIGKILL):
    """
    Steps that mocks a crash py sending a signal to its own process
    """
    os.kill(os.getpid(), sig)

class RefCounted: # pylint: disable=too-few-public-methods
    """
    Class that tracks its number of living instances
    """
    count = 0
    def __init__(self):
        self.last_count = self.count
        RefCounted.count += 1
        logging.info('Creating refcounted object, counter now %d', RefCounted.count)

    def __del__(self):
        RefCounted.count -= 1
        logging.info('Destroying refcounted object, counter now %d', RefCounted.count)
@step
def refcount(dummy, fail=True):
    """
    Create an instance of a reference-counted class and returns instance number
    """

    del dummy # only used to simulate different step
    obj = RefCounted()

    if fail:
        raise ValueError
    return obj.count

@step
def alloc_mem(N, dummy): # pylint: disable=invalid-name
    """
    Tries to alloc N bytes of memory
    """
    del dummy # only to make different tasks
    proc = psutil.Process()
    logging.info('VM: %d', proc.memory_info().vms)
    some_array = np.zeros(N // 8)
    logging.info('VM: %d', proc.memory_info().vms)
    return some_array.sum()

@step
def sum_dict(some_dict):
    """
    Sums the values in a dict, ignoring keys, by iteration
    """
    tot = 0
    for key in some_dict:
        tot += some_dict[key]
    return tot

def empty() -> list:
    """Trivial empty target for CLI galp.client """
    return []

@step
def meta(values):
    """
    A list of tasks
    """
    return [
        identity(value) for value in values
        ]

@step
def meta_meta(values):
    """
    More complicated meta graph involving a literal in between steps
    """
    intermediate_list =  [
        identity(value) for value in values
        ]
    return meta(intermediate_list)

@step
def meta_error():
    """
    A valid meta step returning a task which itself fails
    """
    return raises_error()

@step
def echo(to_stdout: str, to_stderr: str):
    print(to_stdout, file=sys.stdout)
    print(to_stderr, file=sys.stderr)


def double_upload():
    """
    Diamond pattern to a non-trivial literal with different leg branches.

    Reproduction of a wild bug that exposed a problem with the double upload
    logic.
    """
    task_a = hello()
    task_b = identity([task_a])

    return task_b, identity(task_b)
