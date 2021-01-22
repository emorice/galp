"""
Type hints utils, used to add metadata about parameters and return values of
steps.
"""

from typing import *

"""
Type indicating numeric packed data that should for instance not be serialized to
readable text for performance reasons.
"""
ArrayLike = NewType('ArrayLike', Any)
