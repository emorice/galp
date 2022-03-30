"""
Steps only used for testing, and loaded through the plugin system.
"""
import logging
import os
import time

import numpy as np
import pyarrow as pa

from typing import Tuple

from galp.graph import StepSet
from galp.typing import ArrayLike, Table

export = StepSet()

# Alternative namespaces to register the same function several times
export2 = StepSet()
export3 = StepSet()

@export.step
def plugin_hello():
    return '6*7'

@export.step()
def alt_decorator_syntax():
    return 'alt'

def tag_me():
    return 'tagged'

# Only this one can be called
tagged1 = export.step(vtag=0)(tag_me)

untagged = export2.step(tag_me)
tagged2 = export3.step(vtag=1)(tag_me)

def naive_fib(n):
     if n == 1 or n == 2:
         return 1
     else:
         return naive_fib(n-1) + naive_fib(n-2)

@export.step
def profile_me(n):
    return naive_fib(n)

@export.step
def arange(n) -> ArrayLike:
    """Numpy return type"""
    return np.arange(n)

@export.step
def npsum(v: ArrayLike):
    """
    Numpy input but generic return type
    """
    # Wrong, returns a numpy type
    # return v.sum()
    return float(v.sum())

@export.step
def some_table() -> Table:
    return  pa.table({
        'x': ['abc', 'def', 'gh'],
        'y': [1.0, -0.0, 7]
        })

@export.step
def some_tuple() -> Tuple[ArrayLike, int]:
    return (
        np.arange(10),
        10
    )

@export.step
def native_tuple() -> Tuple[str, int]:
    return (
        'fluff',
        14
    )

@export.step
def raises_error():
    return 1 / 0

@export.step
def some_numerical_list() -> ArrayLike:
    return [1., 2.5, 3.]

@export(items=3)
def light_syntax():
    return 5, ["a", "b"], {'x': 7.}

@export(items=2)
def raises_error_multiple():
    """
    Like raises_error, but returns an iterable handle
    """
    return True, 1 / 0

@export
def sleeps(secs, some_arg):
    time.sleep(secs)
    return some_arg
    
@export
def sum_variadic(*args):
    return sum(args)

@export
def busy_loop():
    while True:
        pass

@export
def suicide(sig):
    os.kill(os.getpid(), sig)

class RefCounted:
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

    obj = RefCounted()

    if fail:
        raise ValueError
    else:
        return obj.count
