"""
General types and registration machinery used by most layers
"""

# Type registration machinery
# ===========================

class MessageType: # pylint: disable=too-few-public-methods
    """
    Base class that requires subclasses to provide a key argument and store it.

    Unions of MessageType subclasses can then be coveniently serialized and
    deserialized.
    """
    _message_type_key: bytes = b'_root'

    def __init_subclass__(cls, /, key: str | None, **kwargs) -> None:
        # Perform other initializations
        super().__init_subclass__(**kwargs)
        # Bypass formal classes
        if key is None:
            return
        # Normalize the key
        b_key = key.lower().encode('ascii')
        # Save the key
        cls._message_type_key = b_key

    @classmethod
    def message_get_key(cls) -> bytes:
        """
        Get the unique key associated with a class
        """
        return cls._message_type_key
