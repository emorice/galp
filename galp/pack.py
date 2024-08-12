"""
Utils to pack and unpack trees of dataclass instances

Generalize asdict to handle unions, out-of-band data, and loading of list of
dataclass instances
"""

from dataclasses import field, fields, is_dataclass
from functools import singledispatch
from typing import TypeVar, get_args, get_origin, Any, Annotated

import msgpack # type:ignore[import-untyped]

class _Annotation: # pylint: disable=too-few-public-methods
    """
    Type for private type annotations
    """

_IsPayload = _Annotation()

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
            raise TypeError(obj)
        self.get_key = _get_key
        for key, cls in types.items():
            self.from_key[key] = cls
            self.get_key.register(cls, lambda _obj, key=key: key)

    def dump_part(self, obj: object) -> tuple[object, list[bytes]]:
        """Dump an object with the union member key"""
        obj_doc, extras = dump_part(obj)
        return (self.get_key(obj), obj_doc), extras

    def load_part(self, doc: tuple[str, object], stream: list[bytes]
                  ) -> tuple[object, list[bytes]]:
        """Load a union member according to key"""
        key, value_doc = doc
        return load_part(self.from_key[key], value_doc, stream)


L = TypeVar('L', bound=list)
def load_list(cls: type[L], doc: list, stream: list[bytes]
             ) -> tuple[L, list[bytes]]:
    """Load a list"""
    obj = cls()
    type_var, = get_args(cls)
    for item_doc in doc:
        item, stream = load_part(type_var, item_doc, stream)
        obj.append(item)
    return obj, stream

def check_payload(cls):
    """Parse and strip paylaod annotation"""
    if _IsPayload in getattr(cls, '__metadata__', []):
        return get_args(cls)[0], True
    return cls, False

T = TypeVar('T')
def load_part(cls: type[T], doc: object, stream: list[bytes]
              ) -> tuple[T, list[bytes]]:
    """
    Comsume an object of type `cls` from the docstream.
    Depending on number of payloads, there may be leftover items that are returned.
    """
    # Trivial guard for basic types
    if not is_dataclass(cls):
        orig = get_origin(cls)
        if orig and issubclass(orig, list):
            if not isinstance(doc, list):
                raise TypeError('list expected')
            # mypy doesn't like the narrowing of cls
            return load_list(cls, doc, stream) # type: ignore[type-var]
        if not isinstance(doc, cls):
            raise TypeError
        return doc, stream # type: ignore[return-value]

    if not isinstance(doc, dict):
        raise TypeError
    obj_dict = dict(doc)
    for f in fields(cls):
        item_cls, is_payload = check_payload(f.type)
        if is_payload:
            # Get data for this attribute from the rest of the stream
            attr_doc_buf, *stream = stream
            # Deserialize it, but shortcut bytes object
            if item_cls is bytes:
                attr_doc = attr_doc_buf
            else:
                attr_doc = msgpack.loads(attr_doc_buf)
        else:
            # Get data from the main dict
            attr_doc = obj_dict[f.name]

        # Then apply the corresponding loader
        # The loader can consume further stream items
        if tmap := f.metadata.get('typemap'):
            obj_dict[f.name], stream = tmap.load_part(attr_doc, stream)
        else:
            obj_dict[f.name], stream = load_part(item_cls, attr_doc, stream)
    return cls(**obj_dict), stream # type: ignore[return-value] # what

def dump_list(obj: list) -> tuple[list, list[bytes]]:
    """Dump a list"""
    doc = []
    extras: list[bytes] = []
    for item in obj:
        item_doc, item_extras = dump_part(item)
        doc.append(item_doc)
        extras.extend(item_extras)
    return doc, extras

def dump_part(obj: object) -> tuple[object, list[bytes]]:
    """
    Dump an arbitrary object
    """
    # Trivial guard for basic types
    if not is_dataclass(obj):
        if isinstance(obj, list):
            return dump_list(obj)
        return obj, []

    # Extract original attributes
    orig_obj_dict = vars(obj)
    obj_dict = {}
    docstream = []

    for f in fields(obj):
        # Apply the dumper for this attribute
        # The dumper can produce further stream items
        if tmap := f.metadata.get('typemap'):
            attr_doc, extras = tmap.dump_part(orig_obj_dict[f.name])
        else:
            attr_doc, extras = dump_part(orig_obj_dict[f.name])

        item_cls, is_payload = check_payload(f.type)
        if is_payload:
            # Put data for this attribute in the rest of the stream
            if item_cls is bytes:
                attr_doc_buf = attr_doc
            else:
                attr_doc_buf = msgpack.dumps(attr_doc)
            docstream.append(attr_doc_buf)
        else:
            # Put data in the main dict
            obj_dict[f.name] = attr_doc
        docstream.extend(extras)

    return obj_dict, docstream

def dump(obj: object) -> list[bytes]:
    """Dump object and finalize root doc"""
    root, extras = dump_part(obj)
    return [msgpack.dumps(root), *extras]

def load(cls, msg: list[bytes]):
    """Load object from finalized root doc"""
    root_buf, *extras = msg
    return load_part(cls, msgpack.loads(root_buf), extras)

# The two functions below return Fields, but type checker get confused if it
# knows that
def union(types):
    """Mark a union field and the associated keys"""
    # pylint: disable=invalid-field-call # We're defining a field
    return field(metadata={'typemap': TypeMap(types)})
