"""
Exercise multiple steps with diverse output
"""

import sys
import time
from tqdm.auto import tqdm

import galp

@galp.step
def progress(i):
    """
    Progress bar
    """
    for _ in tqdm(range(i)):
        time.sleep(1)

@galp.step
def lines(i):
    """
    Alternating out/err lines
    """
    print('Something to stout', i, flush=True)
    time.sleep(1)
    print('Something to stderr', i, file=sys.stderr, flush=True)
    time.sleep(1)

@galp.step
def likes_odd(i):
    """
    Succeeds only in odd numbers
    """
    time.sleep(1)
    if not i % 2:
        raise ValueError


def all_tasks(n):
    """
    Collection of all of the above
    """
    return [
            (progress(i), lines(i), likes_odd(i))
            for i in range(n)
            ]
