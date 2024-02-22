"""
Common logic for parsing messages
"""

from types import UnionType, GenericAlias
from typing import TypeVar, Protocol, Callable, Any, TypeAlias, Generic

from galp.result import Result
from galp.serializer import load_model, LoadError
from .types import MessageType

T = TypeVar('T')

Loader: TypeAlias = Callable[[list[bytes]], Result[T, LoadError]]

class DefaultLoader(Protocol): # pylint: disable=too-few-public-methods
    """Generic callable signature for a universal object loader"""
    def __call__(self, cls: type[T]) -> Loader[T]: ...

class LoaderDict:
    """
    Thin wrapper around a dictionary that maps types to a method to load them
    from buffers. This class is used because even if the functionality is a
    trivial defaultdict, the type signature is quite more elaborate.
    """
    def __init__(self, _default_loader: DefaultLoader):
        self._loaders: dict[type, Callable[[list[bytes]], Any]] = {}
        self._default_loader = _default_loader

    def __setitem__(self, cls: type[T], loader: Loader[T]) -> None:
        self._loaders[cls] = loader

    def __getitem__(self, cls: type[T]) -> Loader[T]:
        loader = self._loaders.get(cls)
        if loader is None:
            return self._default_loader(cls)
        return loader

def default_loader(cls: type[T]) -> Loader[T]:
    """
    Fallback to loading a message consisting of a single frame by using the
    general pydantic validating constructor for e.g. dataclasses
    """
    def _load(frames: list[bytes]) -> Result[T, LoadError]:
        match frames:
            case [frame]:
                return load_model(cls, frame)
            case _:
                return LoadError('Wrong number of frames')
    return _load

MT = TypeVar('MT', bound=MessageType)

def _make_registry(union: UnionType) -> dict[bytes, type[MessageType]]:
    if not isinstance(union, UnionType):
        raise RuntimeError('Concrete UnionLoader parameter must be a union type')

    registry: dict[bytes, type[MessageType]] = {}
    for mem_cls in union.__args__:
        b_key = mem_cls.message_get_key()
        previous = registry.get(b_key)
        if previous:
            raise ValueError(f'Message type key {b_key!r} is already used for '
                    + str(previous))
        registry[b_key] = mem_cls
    return registry

class UnionLoader(Generic[MT]):
    """
    Helper generic factory class to deserialize union members

    Subclass this class, specialize the type var on some union of message types,
    and override the loaders class attribute to inject the loaders for the union
    member types.
    """
    loaders: LoaderDict = LoaderDict(default_loader)

    _member_registry: dict[bytes, type[MT]]

    @classmethod
    def load(cls, frames: list[bytes]) -> Result[MT, LoadError]:
        """Attempt to load an object of the specified type"""
        match frames:
            case [type_frame, *data_frames]:
                sub_cls = cls._member_registry.get(type_frame)
                if sub_cls is None:
                    return LoadError('Bad message:'
                            + f' {cls.__module__}.{cls.__qualname__} has no'
                            + f' subclass with key {type_frame!r}')
                return cls.loaders[sub_cls](data_frames)
            case _:
                return LoadError('Bad message: Wrong number of frames')

    @classmethod
    def get_type(cls, key: bytes) -> type[MT] | None:
        """Return union member with given key, if any"""
        return cls._member_registry.get(key)

    def __class_getitem__(cls, item):
        orig = GenericAlias(cls, item)

        # Inheriting, nothing to do
        if isinstance(item, TypeVar):
            return orig

        # Specializing on concrete type var, copy class and set member types
        # registry
        class _Loader(orig): # pylint: disable=too-few-public-methods
            _member_registry = _make_registry(item)
        return _Loader
