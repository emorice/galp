"""
Abstraction of serialization

Many code paths may need to deal with an object that is serialized or with a
serializer but do not depend on what is the actual serialization strategy used.
"""

from dataclasses import dataclass
from typing import Any, TypeVar, Generic, Callable

Nat = TypeVar('Nat')
Ref = TypeVar('Ref')

class Serializer(Generic[Nat, Ref]):
    """
    Abstraction for a serialization strategy

    The class has two generics to be defined and handled by subclasses. Nat
    objects are objects that should be handled out of band in a custom way by
    the serializer. Ref are objects created as the result of such handling,
    to be understood as references to out-of-band data.

    In practice, Nat would be nodes in a task graph and Ref the corresponding
    task name.
    """

    @classmethod
    def loads(cls, data: bytes, native_children: list[Any]) -> Any:
        """
        Unserialize the data in the payload, possibly using metadata from the
        handle.

        Re-raises DeserializeError on any exception.
        """
        raise NotImplementedError


    @classmethod
    def dumps(cls, obj: Any, save: Callable[[Nat], Ref]
              ) -> tuple[bytes, list[Ref]]:
        """
        Serialize the data.

        Args:
            obj: object to serialize
            save: callback to handle nested Nat (task) objects. The callback should
                return a Ref after and only after ensuring that the
                task information will been made available to the recipient of the
                serialized objects, in order to never create serialized objects
                with unresolvable task references.
        """
        raise NotImplementedError


@dataclass
class Serialized(Generic[Ref]):
    """
    Represents a serialized object alongside with the interface needed to
    deserialize it
    """
    serializer: Serializer[Any, Ref]
    data: bytes
    children: list[Ref]

    def deserialize(self, children: list[Any]) -> Any:
        """
        Given the native objects that children are references to, deserialize
        this resource by injecting the corresponding objects into it.
        """
        return self.serializer.loads(self.data, children)
