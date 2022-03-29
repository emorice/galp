"""
Galp, a network-based incremental pipeline runner with tight python integration.
"""
from .client import Client, TaskFailedError
from .graph import StepSet
from .serializer import DeserializeError
