"""
Abstraction of serialization

Many code paths may need to deal with an object that is serialized or with a
serializer but do not depend on what is the actual serialization strategy used.
"""

from typing import Any, TypeVar, Generic, Callable, Sequence
from dataclasses import dataclass
from galp.result import Ok, Error


class LoadError(Error[str]):
    """Error value to be returned on failed deserialization"""

Nat = TypeVar('Nat')
Ref = TypeVar('Ref')

Ref_co = TypeVar('Ref_co', covariant=True)

@dataclass(frozen=True)
class GenSerialized(Generic[Ref_co]):
    """
    A serialized object

    Attributes:
        data: the serialized object data
        children: the subordinate object references that are linked from within the
            serialized data
    """
    data: bytes
    children: Sequence[Ref_co]
    _loads: Callable[[bytes, Sequence[object]], Ok[object] | LoadError]

    def deserialize(self, children: Sequence[object]) -> Ok[object] | LoadError:
        """
        Given the native objects that children are references to, deserialize
        this resource by injecting the corresponding objects into it.
        """
        return self._loads(self.data, children)

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
    def loads(cls, data: bytes, native_children: list[Any]
            ) -> Ok[object] | LoadError:
        """
        Unserialize the data in the payload, possibly using metadata from the
        handle.
        """
        raise NotImplementedError

    @classmethod
    def dumps(cls, obj: Any, save: Callable[[Nat], Ref]
              ) -> GenSerialized[Ref]:
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
