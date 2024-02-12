"""
General types and registration machinery used by most layers
"""

from typing_extensions import Self

# Type registration machinery
# ===========================

class MessageType:
    """
    Base class that implement mandatory registration of direct subclasses for
    all its subclass hierarchy.

    Every class that subclasses, directly or indirectly, MessageType, has to
    specify a unique key that identifies it among its direct sister classes.
    Unicity is enforced at class definition time.

    Class methods are available to obtain the key a class has been registered
    with, and crucially which direct subclass of a given class is associated
    with a key.

    A class can be registered with a key of None. In that case, it is
    transparent to the registry mechanism, subclasses of the None-key class will
    be registered on the base class of the None-key class. This can be used to
    insert types in the hierarchy and exploit inheritance without creating new
    namespaces.
    """
    _message_registry: dict[bytes, type[Self]] = {}
    _message_type_key: bytes = b'_root'

    def __init_subclass__(cls, /, key: str | None) -> None:
        # Bypass formal classes
        if key is None:
            return
        # Normalize the key
        b_key = key.lower().encode('ascii')
        # First, register the class in its parent's regitry
        previous = cls._message_registry.get(b_key)
        if previous:
            raise ValueError(f'Message type key {b_key!r} is already used for '
                    + str(previous))
        cls._message_registry[b_key] = cls
        # Save the key, since accesing the parent's registry later is hard
        cls._message_type_key = b_key
        # Then, initialize a new registry that will mask the parent's
        cls._message_registry = {}

    @classmethod
    def message_get_key(cls) -> bytes:
        """
        Get the unique key associated with a class
        """
        return cls._message_type_key

    @classmethod
    def message_get_type(cls, key: bytes) -> type[Self] | None:
        """
        Get the sub-class associated with a unique key
        """
        # The type checker is right that in general, _message_registry could
        # contain a type that is not a sub-type of Self, and complains.
        # In reality, the fact that this cannot happen is application logic
        # guaranteed by the fact that we *reset* the registry each time, and
        # therefore the hierarchy above is never exposed to the class.
        return cls._message_registry.get(key) # type: ignore[return-value]
