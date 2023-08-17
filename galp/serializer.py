"""
Serialization utils
"""

from __future__ import annotations

from typing import Any, List, Callable, TypeVar, Generic
import logging

import msgpack # type: ignore[import] # Issue 85
import dill # type: ignore[import] # Issue 85

from pydantic import BaseModel, ValidationError, TypeAdapter, RootModel

class DeserializeError(ValueError):
    """
    Exception raised to wrap any error encountered by the deserialization
    backends
    """
    def __init__(self, msg=None):
        super().__init__(msg or 'Failed to deserialize')

Nat = TypeVar('Nat')
Ref = TypeVar('Ref')

class Serializer(Generic[Nat, Ref]):
    """
    Abstraction for a serialization strategy

    The class has two generics to be defined and handled by subclasses. Nat
    objects are objects that should be handled out of band in a custom way by
    the serializer. Ref are objects created as the result of such handling,
    to be understood as references to out-of-band data.
    """

    @classmethod
    def loads(cls, data: bytes, native_children: List[Any]) -> Any:
        """
        Unserialize the data in the payload, possibly using metadata from the
        handle.

        Re-raises DeserializeError on any exception.
        """
        try:
            return msgpack.unpackb(data, ext_hook=cls._ext_hook(native_children),
                    raw=False, use_list=False)
        except Exception as exc:
            raise DeserializeError from exc

    @classmethod
    def dumps(cls, obj: Any, save: Callable[[Nat], Ref]
              ) -> tuple[bytes, list[Ref]]:
        """
        Serialize the data.

        Note that this always uses the default serializer. This is because
        higher-level galp resources like lists of resources are normally
        serialized one by one, so the higher-level object is never directly
        passed down to the serializer.

        Args:
            obj: object to serialize
            save: callback to handle nested Nat (task) objects. The callback should
                return a Ref after and only after ensuring that the
                task information will been made available to the recipient of the
                serialized objects, in order to never create serialized objects
                with unresolvable task references.
        """
        children : list[Ref] = []
        # Modifies children in place
        payload = msgpack.packb(obj, default=cls._default(children, save), use_bin_type=True)
        return payload, children

    @classmethod
    def as_nat(cls, obj: Any) -> Nat | None:
        """
        Attempts to cast obj to an object that can be handled out of band.

        This must be implemented by concrete subclasses
        """
        raise NotImplementedError

    @classmethod
    def _default(cls, children: list[Ref], save: Callable[[Nat], Ref]
            ) -> Callable[[Any], msgpack.ExtType]:
        """
        Out-of-band sub-tasks and dill fallback

        Modifies children in-place
        """
        def _children_default(obj: Any) -> msgpack.ExtType:
            nat = cls.as_nat(obj)
            if nat is not None:
                index = len(children)
                children.append(save(nat))
                return msgpack.ExtType(
                    _CHILD_EXT_CODE,
                    index.to_bytes(4, 'little')
                    )

            return msgpack.ExtType(
                    _DILL_EXT_CODE,
                    dill.dumps(obj)
                    )
        return _children_default

    @classmethod
    def _ext_hook(cls, native_children):
        """
        ExtType to object for msgpack
        """
        def _hook(code, data):
            if code == _CHILD_EXT_CODE:
                if not len(data) == 4:
                    raise DeserializeError(f'Invalid child index {data}')
                index = int.from_bytes(data, 'little')
                try:
                    return native_children[index]
                except (KeyError, IndexError) as exc:
                    raise DeserializeError(f'Missing child index {data}') from exc
            if code == _DILL_EXT_CODE:
                logging.warning('Unsafe deserialization with dill')
                return dill.loads(data)
            raise DeserializeError(f'Unknown ExtType {code}')
        return _hook

def serialize_child(index):
    """
    Serialized representation of a reference to the index-th child task of an
    object.

    This is also used when storing scattered tasks.
    """
    return msgpack.ExtType(
        _CHILD_EXT_CODE,
        index.to_bytes(4, 'little')
        )

def dump_model(model: Any, exclude: set[str] | None = None) -> bytes:
    """
    Serialize pydantic model or dataclass with msgpack

    Args:
        model: the dataclass or pydantic model to dump
        exclude: set of fields to exclude from the serialization
    """
    if hasattr(model, 'model_dump'):
        dump = model.model_dump(exclude=exclude)
    else:
        dump = (
                RootModel[type(model)] # type: ignore[misc] # pydantic magic
                (model).model_dump(exclude=exclude)
                )
    return msgpack.dumps(dump)

T = TypeVar('T', bound=BaseModel)

def load_model(model_type: type[T], payload: bytes, **extra_fields: Any
        ) -> T:
    """
    Load a msgpack-serialized pydantic model

    Also works on union of models (though the call won't type check).

    Args:
        model_type: The pydantic model class of the object to create
        payload: The msgpack-encoded buffer with the object data
        extra_fieds: attributes to add to the decoded payload before validation,
            intended to add out-of-band fields to the structure.

    Raises:
        DeserializeError on any failed deserialization or validation.
    """
    try:
        doc = msgpack.loads(payload)
        if not isinstance(doc, dict):
            raise TypeError
        doc.update(extra_fields)
    # Per msgpack docs:
    # "unpack may raise exception other than subclass of UnpackException.
    # If you want to catch all error, catch Exception instead.:
    except Exception as exc: # pylint: disable=broad-except
        err = 'Invalid msgpack message'
        logging.exception(err)
        raise DeserializeError(err) from exc

    try:
        return TypeAdapter(model_type).validate_python(doc)
    except ValidationError as exc:
        err = 'Invalid model data'
        logging.exception(err)
        raise DeserializeError(err) from exc

# Private serializer helpers
# ==========================
# This should not be exposed to the serializer consumers

_CHILD_EXT_CODE = 0
_DILL_EXT_CODE = 1
