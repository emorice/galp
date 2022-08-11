"""
Steps only used for testing, and loaded through the plugin system.
"""
import logging
import os
import time

import psutil
import numpy as np
import pyarrow as pa

from galp.graph import StepSet

export = StepSet()

# Alternative namespaces to register the same function several times
export2 = StepSet()
export3 = StepSet()

@export.step
def plugin_hello():
    """
    Takes no arguments, returns a constant, simplest step
    """
    return '6*7'

@export.step()
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

# Only this one can be called
tagged1 = export.step(vtag=0)(tag_me)

untagged = export2.step(tag_me)
tagged2 = export3.step(vtag=1)(tag_me)

def naive_fib(n): # pylint: disable=invalid-name
    """
    Recursive fibonacci calculation, useful to put some load on the interpreter.
    """
    if n in (1, 2):
        return 1
    return naive_fib(n-1) + naive_fib(n-2)

@export.step
def profile_me(n): # pylint: disable=invalid-name
    """
    Wrapper around `naive_fib`
    """
    return naive_fib(n)

@export.step
def arange(n): # pylint: disable=invalid-name
    """Numpy return type"""
    return np.arange(n)

@export.step
def npsum(vect):
    """
    Numpy input but generic return type
    """
    return float(vect.sum())

@export.step
def some_table():
    """
    Pyarrow return type
    """
    return  pa.table({
        'x': ['abc', 'def', 'gh'],
        'y': [1.0, -0.0, 7]
        })

@export.step(items=2)
def some_tuple():
    """
    Returns two resources that can be addressed separately
    """
    return (
        np.arange(10),
        10
    )

@export.step(items=2)
def native_tuple():
    """
    Returns two resources that can be addressed separately
    """
    return (
        'fluff',
        14
    )

@export.step
def raises_error():
    """
    Never completes, raises a divide-by-zero error
    """
    return 1 / 0

@export(items=3)
def light_syntax():
    """
    Uses directly the StepSet as a decorator
    """
    return 5, ["a", "b"], {'x': 7.}

@export(items=2)
def raises_error_multiple():
    """
    Like raises_error, but returns an iterable handle
    """
    return True, 1 / 0

@export
def sleeps(secs, some_arg):
    """
    Sleeps for the given time and return the argument unchanged
    """
    time.sleep(secs)
    return some_arg

@export
def sum_variadic(*args):
    """
    Step that takes variadic positional args
    """
    return sum(args)

@export
def busy_loop():
    """
    Infinite pure python loop
    """
    while True:
        pass

@export
def suicide(sig):
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
@export
def refcount(dummy, fail=True):
    """
    Create an instance of a reference-counted class and returns instance number
    """

    del dummy # only used to simulate different step
    obj = RefCounted()

    if fail:
        raise ValueError
    return obj.count

@export
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
