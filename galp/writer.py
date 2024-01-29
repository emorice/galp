"""
Definition of a swappable layer writer.

Since sessions can be composed in an arbitrary order, they are by design not
type safe. Therefore, they must always be encapsulated inside higher-level,
type-safe interfaces according to the network stack design before being given to
applications.
"""
from typing import Callable, TypeAlias

TransportMessage: TypeAlias = list[bytes]
"""
Type of messages expected by the transport, also somewhat ZMQ-specific
"""

Writer: TypeAlias = Callable[[list[bytes]], TransportMessage]
