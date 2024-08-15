"""
Utils to pack and unpack trees of dataclass instances

Generalize asdict to handle unions, out-of-band data, and loading of list of
dataclass instances
"""

import logging
from dataclasses import fields, is_dataclass
from functools import singledispatch
from typing import TypeVar, get_args, get_origin, Any, Annotated, Literal
from types import UnionType, NoneType

import msgpack # type:ignore[import-untyped]

from galp.result import Ok, Result
from galp.serialize import LoadError

class ValidationError(TypeError):
    """
    Error raised on failed validation
    """

_IsPayload = object()

U = TypeVar('U')
Payload = Annotated[U, _IsPayload]

class TypeMap:
    """
    Bi-directional mapping between keys and types
    """
    def __init__(self, types: dict[str, Any]):
        self.from_key = {}
        @singledispatch
        def _get_key(obj):
            raise ValidationError(obj)
        self.get_key = _get_key
        for key, cls in types.items():
            self.from_key[key] = cls
            self.get_key.register(cls, lambda _obj, key=key: key)

    def dump_part(self, obj: object) -> tuple[object, list[bytes]]:
        """Dump an object with the union member key"""
        obj_doc, extras = dump_part(obj)
        return (self.get_key(obj), obj_doc), extras

    def dump(self, obj: object) -> list[bytes]:
        """Dump object and member key and finalize root doc"""
        root, extras = self.dump_part(obj)
        return [msgpack.dumps(root), *extras]

    def load_part(self, doc: tuple[str, object], stream: list[bytes]
                  ) -> tuple[object, list[bytes]]:
        """Load a union member according to key"""
        key, value_doc = doc
        return load_part(self.from_key[key], value_doc, stream)

    def load(self, msg: list[bytes]) -> Result:
        """Load union member from finalized root doc"""
        root_buf, *extras = msg
        try:
            obj, extras = self.load_part(msgpack.loads(root_buf), extras)
        except ValidationError:
            logging.exception('Validation failed')
            return LoadError('Validation failed')
        if extras:
            return LoadError('Extra data at end of input')
        return Ok(obj)

L = TypeVar('L', list, tuple)
def load_list(cls: type[L], doc: object, stream: list[bytes]
             ) -> tuple[L, list[bytes]]:
    """Load a list or tuple"""
    if not isinstance(doc, list):
        raise ValidationError('list expected')
    obj = []
    type_var, *_ellipsis = get_args(cls)
    assert _ellipsis in ([], [...]), 'not supported yet'
    for item_doc in doc:
        item, stream = load_part(type_var, item_doc, stream)
        obj.append(item)
    return cls(obj), stream

D = TypeVar('D', bound=dict)
def load_dict(cls: type[D], doc: object, stream: list[bytes]
             ) -> tuple[D, list[bytes]]:
    """Load a dict"""
    if not isinstance(doc, dict):
        raise ValidationError('dict expected')
    obj = cls()
    key_type_var, value_type_var = get_args(cls)
    for key_item_doc, value_item_doc in doc.items():
        key_item, stream = load_part(key_type_var, key_item_doc, stream)
        value_item, stream = load_part(value_type_var, value_item_doc, stream)
        obj[key_item] = value_item
    return obj, stream

def check_payload(cls):
    """Parse and strip paylaod and other annotations"""
    if get_origin(cls) is Annotated:
        orig_cls, *annotations = get_args(cls)
        return orig_cls, _IsPayload in annotations
    return cls, False

_FIELDS_CACHE: dict[object, list[tuple[str, bool, TypeMap | None, Any]]] = {}

def parse_fields(cls):
    """
    Extract relevant metadata from dataclass fields
    """
    field_list = []

    for f in fields(cls):
        orig_cls, is_payload = check_payload(f.type)
        # Generate a type map for unions
        if isinstance(orig_cls, UnionType):
            typemap = TypeMap(dict(enumerate(get_args(orig_cls))))
        else:
            typemap = None

        field_list.append((
            f.name,
            is_payload,
            typemap,
            orig_cls))

    _FIELDS_CACHE[cls] = field_list
    return field_list

T = TypeVar('T')
def load_builtin(cls: type[T], doc: object, stream: list[bytes]
              ) -> tuple[T, list[bytes]]:
    """
    Load basic (non-dataclass) types
    """
    orig = get_origin(cls)
    if orig in (list, tuple):
        # mypy doesn't like this way of narrowing cls
        return load_list(cls, doc, stream) # type: ignore[type-var]
    if orig is dict:
        return load_dict(cls, doc, stream) # type: ignore[type-var]
    if orig is Literal:
        value, = get_args(cls)
        if doc != value:
            raise ValidationError(f'{value} expected')
        return value, stream # type: ignore[call-arg]
    if cls is NoneType:
        if doc is not None:
            raise ValidationError('None expected')
        return None, stream # type: ignore[return-value]
    # For all remaining basic types, we assume they can be coerced by calling
    # the constructor: int, float, bool, str, bytes
    return cls(doc), stream # type: ignore[call-arg]

def load_part(cls: type[T], doc: object, stream: list[bytes]
              ) -> tuple[T, list[bytes]]:
    """
    Comsume an object of type `cls` from the docstream.
    Depending on number of payloads, there may be leftover items that are returned.
    """
    # Trivial guard for basic types
    if not is_dataclass(cls):
        return load_builtin(cls, doc, stream)

    if not isinstance(doc, dict):
        raise ValidationError
    obj_dict = dict(doc)
    try:
        field_list = _FIELDS_CACHE[cls]
    except KeyError:
        field_list = parse_fields(cls)

    for name, is_payload, typemap, item_cls in field_list:
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
        if typemap:
            obj_dict[name], stream = typemap.load_part(attr_doc, stream)
        else:
            obj_dict[name], stream = load_part(item_cls, attr_doc, stream)
    return cls(**obj_dict), stream # type: ignore[return-value] # what

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

    for name, is_payload, typemap, item_cls in field_list:
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
                    raise ValidationError
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
    except ValidationError:
        logging.exception('Validation failed')
        return LoadError('Validation failed')
    if extras:
        return LoadError('Extra data at end of input')
    return Ok(obj)
