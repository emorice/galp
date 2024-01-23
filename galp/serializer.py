"""
Serialization utils

This should be renamed to make more obvious that this is specific implementation
of the serialization interface.
"""

from __future__ import annotations

from typing import Any, List, Callable, TypeVar
import logging

import msgpack # type: ignore[import] # Issue 85
import dill # type: ignore[import] # Issue 85

from pydantic import ValidationError, TypeAdapter, RootModel

from galp import serialize
from galp.serialize import LoadError, Ok

Nat = TypeVar('Nat')
Ref = TypeVar('Ref')

class Serializer(serialize.Serializer[Nat, Ref]):
    """
    Implementation of a serialization based on msgpack and dill, with support
    for further customization through an out-of-band mechanism.

    The class has two generics to be defined and handled by subclasses. Nat
    objects are objects that should be handled out of band in a custom way by
    the serializer. Ref are objects created as the result of such handling,
    to be understood as references to out-of-band data.
    """

    @classmethod
    def loads(cls, data: bytes, native_children: List[Any]) -> Ok | LoadError:
        """
        Unserialize the data in the payload, possibly using metadata from the
        handle.

        Re-raises DeserializeError on any exception.
        """
        try:
            return Ok(msgpack.unpackb(data, ext_hook=cls._ext_hook(native_children),
                    raw=False, use_list=False))
        except Exception: # pylint: disable=broad-exception-caught
            # (msgpack limitation)
            logging.exception('Invalid msgpack')
            return LoadError('Invalid msgpack')

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
        ExtType to object for msgpack. This raises on problems, as this runs as
        a callback inside msgpack.
        """
        def _hook(code, data):
            if code == _CHILD_EXT_CODE:
                if not len(data) == 4:
                    raise ValueError(f'Invalid child index {data}')
                index = int.from_bytes(data, 'little')
                try:
                    return native_children[index]
                except (KeyError, IndexError) as exc:
                    raise ValueError(f'Missing child index {data}') from exc
            if code == _DILL_EXT_CODE:
                return dill.loads(data)
            raise ValueError(f'Unknown ExtType {code}')
        return _hook

def serialize_child(index: int) -> msgpack.ExtType:
    """
    Serialized representation of a reference to the index-th child task of an
    object.

    This is also used when storing scattered tasks.
    """
    return msgpack.ExtType(
        _CHILD_EXT_CODE,
        index.to_bytes(4, 'little')
        )

def dump_model(model: Any) -> bytes:
    """
    Serialize pydantic model or dataclass with msgpack

    Args:
        model: the dataclass or pydantic model to dump
    """
    if hasattr(model, 'model_dump'):
        dump = model.model_dump()
    else:
        dump = (
                RootModel[type(model)] # type: ignore[misc] # pydantic magic
                (model).model_dump()
                )
    return msgpack.dumps(dump)

T = TypeVar('T')

def load_model(model_type: type[T], payload: bytes) -> T | LoadError:
    """
    Load a msgpack-serialized pydantic model

    Also works on dataclasses, or union of models (though for unions the call
    won't type check because an union is not a Type).

    Args:
        model_type: The pydantic model class of the object to create
        payload: The msgpack-encoded buffer with the object data

    Returns:
        The deserialized object or LoadError
    """
    try:
        doc = msgpack.loads(payload)
    # Per msgpack docs:
    # "unpack may raise exception other than subclass of UnpackException.
    # If you want to catch all error, catch Exception instead.:
    except Exception: # pylint: disable=broad-except
        err = 'Invalid msgpack message'
        logging.exception(err)
        return LoadError(err)

    try:
        return TypeAdapter(model_type).validate_python(doc)
    except ValidationError:
        err = 'Invalid model data'
        logging.exception(err)
        return LoadError(err)

# Private serializer helpers
# ==========================
# This should not be exposed to the serializer consumers

_CHILD_EXT_CODE = 0
_DILL_EXT_CODE = 1
