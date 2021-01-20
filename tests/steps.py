"""
Steps only used for testing, and loaded through the plugin system.
"""

import numpy as np

from galp.graph import StepSet
from galp.typing import ArrayLike

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
