"""
Utils to pack and unpack trees of dataclass instances

Generalize asdict to handle unions, out-of-band data, and loading of list of
dataclass instances
"""

import logging
from dataclasses import fields, is_dataclass
from functools import singledispatch
from typing import (TypeVar, get_args, get_origin, Any, Annotated, Literal,
                    Union, Generic, Callable, TypeAlias)
from collections.abc import Hashable
from types import NoneType, UnionType

import msgpack # type:ignore[import-untyped]

from galp.result import Ok, Error

class LoadError(Error[str]):
    """Error value to be returned on failed deserialization"""

class LoadException(TypeError):
    """
    Error raised on failed validation
    """

_IsPayload = object()

T = TypeVar('T')
Payload = Annotated[T, _IsPayload]

class TypeMap(Generic[T]):
    """
    Bi-directional mapping between keys and types
    """
    def __init__(self, types: dict[Hashable, type[T]]):
        self.from_key = {}
        self.loaders = {}
        @singledispatch
        def _get_key(obj: T) -> Hashable:
            raise LoadException(obj)
        self.get_key = _get_key
        for key, cls in types.items():
            self.from_key[key] = cls
            self.get_key.register(cls, lambda _obj, key=key: key)
            self.loaders[key] = get_loader(cls)

    def dump_part(self, obj: T) -> tuple[object, list[bytes]]:
        """Dump an object with the union member key"""
        obj_doc, extras = dump_part(obj)
        return (self.get_key(obj), obj_doc), extras

    def dump(self, obj: T) -> list[bytes]:
        """Dump object and member key and finalize root doc"""
        root, extras = self.dump_part(obj)
        return [msgpack.dumps(root), *extras]

    def load_part(self, doc: tuple[Hashable, object], stream: list[bytes]
                  ) -> tuple[T, list[bytes]]:
        """Load a union member according to key"""
        key, value_doc = doc
        return self.loaders[key](value_doc, stream)

    def load(self, msg: list[bytes]) -> Ok[T] | LoadError:
        """Load union member from finalized root doc"""
        root_buf, *extras = msg
        try:
            obj, extras = self.load_part(msgpack.loads(root_buf), extras)
        except LoadException:
            logging.exception('Validation failed')
            return LoadError('Validation failed')
        if extras:
            return LoadError('Extra data at end of input')
        return Ok(obj)

    @classmethod
    def from_union(cls, union):
        """Build number-based type map if type is a union"""
        # Union types recognition is complicated by the history of python typing
        if not get_origin(union) in (Union, UnionType):
            raise TypeError
        return cls(dict(enumerate(get_args(union))))

Loader: TypeAlias = Callable[[type[T], list[bytes]], tuple[T, list[bytes]]]

_LOADERS: dict[type, Loader] = {}
"""
Cache the loading function for types already seen
"""

def get_loader(cls: type[T]) -> Loader[T]:
    """Get loader for cls, creating and caching it if necessary"""
    if cls in _LOADERS:
        return _LOADERS[cls]
    if is_dataclass(cls):
        loader = make_load_dataclass(cls)
    else:
        loader = make_load_builtin(cls)
    _LOADERS[cls] = loader
    return loader

L = TypeVar('L', list, tuple)
def make_load_list(cls: type[L]) -> Loader[L]:
    """Load a list or tuple"""

    type_var, *_ellipsis = get_args(cls)
    assert _ellipsis in ([], [...]), 'not supported yet'
    load_item = get_loader(type_var)

    def _load_list(doc: object, stream: list[bytes]
                 ) -> tuple[L, list[bytes]]:
        if not isinstance(doc, list):
            raise LoadException('list expected')
        obj = []
        for item_doc in doc:
            item, stream = load_item(item_doc, stream)
            obj.append(item)
        return cls(obj), stream
    return _load_list

D = TypeVar('D', bound=dict)
def make_load_dict(cls: type[D]) -> Loader[D]:
    """Load a dict"""
    key_type_var, value_type_var = get_args(cls)
    load_key = get_loader(key_type_var)
    load_value = get_loader(value_type_var)
    def _load_dict(doc: object, stream: list[bytes]
                 ) -> tuple[D, list[bytes]]:
        if not isinstance(doc, dict):
            raise LoadException('dict expected')
        obj = cls()
        for key_item_doc, value_item_doc in doc.items():
            key_item, stream = load_key(key_item_doc, stream)
            value_item, stream = load_value(value_item_doc, stream)
            obj[key_item] = value_item
        return obj, stream
    return _load_dict

def make_load_literal(cls: type[T]) -> Loader[T]:
    """Load a literal (constant)"""
    value, = get_args(cls)
    def _load(doc: object, stream: list[bytes]) -> tuple[T, list[bytes]]:
        if doc != value:
            raise LoadException(f'{value} expected')
        return value, stream
    return _load

def load_none(doc: object, stream: list[bytes]) -> tuple[None, list[bytes]]:
    """Load None"""
    if doc is not None:
        raise LoadException('None expected')
    return None, stream

def make_load_default(cls: type[T]) -> Loader[T]:
    """Load other basic types: int, float, bool, str, bytes"""
    def _load(doc: object, stream: list[bytes]) -> tuple[T, list[bytes]]:
        # For all remaining basic types, we assume they can be coerced by
        # calling the constructor
        return cls(doc), stream
    return _load

def make_load_builtin(cls: type[T]) -> Loader[T]:
    """
    Load basic (non-dataclass) types
    """
    orig = get_origin(cls)
    if orig in (list, tuple):
        return make_load_list(cls)
    if orig is dict:
        return make_load_dict(cls)
    if orig is Literal:
        return make_load_literal(cls)
    if cls is NoneType:
        return load_none
    return make_load_default(cls)

def check_payload(cls):
    """Parse and strip paylaod and other annotations"""
    if get_origin(cls) is Annotated:
        orig_cls, *annotations = get_args(cls)
        return orig_cls, _IsPayload in annotations
    return cls, False

_FIELDS_CACHE: dict[object, list[tuple[str, bool, TypeMap | None, Any, Loader]]] = {}

def parse_fields(cls):
    """
    Extract relevant metadata from dataclass fields
    """
    field_list = []

    for f in fields(cls):
        orig_cls, is_payload = check_payload(f.type)
        try:
            typemap = TypeMap.from_union(orig_cls)
        except TypeError:
            typemap = None

        if typemap:
            loader = typemap.load_part
        else:
            loader = get_loader(orig_cls)

        field_list.append((
            f.name,
            is_payload,
            typemap,
            orig_cls,
            loader))

    _FIELDS_CACHE[cls] = field_list
    return field_list

def make_load_dataclass(cls: type[T]) -> Loader[T]:
    """
    Generate a load function for a specific dataclass
    """
    field_list = parse_fields(cls)

    def _load(doc: object, stream: list[bytes]) -> tuple[T, list[bytes]]:
        if not isinstance(doc, dict):
            raise LoadException
        obj_dict = dict(doc)
        for name, is_payload, _typemap, item_cls, loader in field_list:
            if is_payload:
                # Get data for this attribute from the rest of the stream
                attr_doc_buf, *stream = stream
                # Deserialize it, but shortcut bytes object
                if item_cls is bytes:
                    attr_doc: Any = attr_doc_buf
                else:
                    attr_doc = msgpack.loads(attr_doc_buf)
            else:
                # Get data from the main dict
                attr_doc = obj_dict[name]

            # Then apply the corresponding loader
            # The loader can consume further stream items
            obj_dict[name], stream = loader(attr_doc, stream)
        return cls(**obj_dict), stream # type: ignore[return-value] # what
    return _load

def load_part(cls: type[T], doc: object, stream: list[bytes]
              ) -> tuple[T, list[bytes]]:
    """
    Comsume an object of type `cls` from the docstream.
    Depending on number of payloads, there may be leftover items that are returned.
    """
    try:
        return _LOADERS[cls](doc, stream)
    except KeyError:
        pass

    return get_loader(cls)(doc, stream)

def dump_list(obj: list | tuple) -> tuple[list, list[bytes]]:
    """Dump a list or tuple"""
    doc = []
    extras: list[bytes] = []
    for item in obj:
        item_doc, item_extras = dump_part(item)
        doc.append(item_doc)
        extras.extend(item_extras)
    return doc, extras

def dump_dict(obj: dict) -> tuple[dict, list[bytes]]:
    """Dump a dict"""
    doc = {}
    extras: list[bytes] = []
    for key, value in obj.items():
        key_doc, key_extras = dump_part(key)
        extras.extend(key_extras)
        value_doc, value_extras = dump_part(value)
        extras.extend(value_extras)
        doc[key_doc] = value_doc
    return doc, extras

def dump_part(obj: object) -> tuple[object, list[bytes]]:
    """
    Dump an arbitrary object
    """
    # Trivial guard for basic types
    if not is_dataclass(obj):
        if isinstance(obj, list | tuple):
            return dump_list(obj)
        if isinstance(obj, dict):
            return dump_dict(obj)
        return obj, []

    # Extract original attributes
    orig_obj_dict = vars(obj)
    obj_dict = {}
    docstream: list[bytes] = []

    try:
        field_list = _FIELDS_CACHE[type(obj)]
    except KeyError:
        field_list = parse_fields(type(obj))

    for name, is_payload, typemap, item_cls, _loader in field_list:
        # Apply the dumper for this attribute
        # The dumper can produce further stream items
        if typemap:
            attr_doc, extras = typemap.dump_part(orig_obj_dict[name])
        else:
            attr_doc, extras = dump_part(orig_obj_dict[name])

        if is_payload:
            # Put data for this attribute in the rest of the stream
            if item_cls is bytes:
                if not isinstance(attr_doc, bytes):
                    raise LoadException
                attr_doc_buf = attr_doc
            else:
                attr_doc_buf = msgpack.dumps(attr_doc)
            docstream.append(attr_doc_buf)
        else:
            # Put data in the main dict
            obj_dict[name] = attr_doc
        docstream.extend(extras)

    return obj_dict, docstream

def dump(obj: object) -> list[bytes]:
    """Dump object and finalize root doc"""
    root, extras = dump_part(obj)
    return [msgpack.dumps(root), *extras]

def load(cls: type[T], msg: list[bytes]) -> Ok[T] | LoadError:
    """Load object from finalized root doc"""
    root_buf, *extras = msg
    try:
        obj, extras = load_part(cls, msgpack.loads(root_buf), extras)
    except LoadException:
        logging.exception('Validation failed')
        return LoadError('Validation failed')
    if extras:
        return LoadError('Extra data at end of input')
    return Ok(obj)
