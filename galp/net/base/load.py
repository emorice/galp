"""
Common logic for parsing messages
"""

from typing import TypeVar, Protocol, Callable, Any, TypeAlias

from galp.serializer import load_model, LoadError
from .types import MessageType

T = TypeVar('T')

Loader: TypeAlias = Callable[[list[bytes]], T | LoadError]

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
    def _load(frames: list[bytes]) -> T | LoadError:
        match frames:
            case [frame]:
                return load_model(cls, frame)
            case _:
                return LoadError('Wrong number of frames')
    return _load

MT = TypeVar('MT', bound=MessageType)

def parse_message_type(cls: type[MT], loaders: LoaderDict) -> Loader[MT]:
    """
    Common logic in parsing union keyed through MessageType
    """
    def _parse(frames: list[bytes]) -> MT | LoadError:
        match frames:
            case [type_frame, *data_frames]:
                sub_cls = cls.message_get_type(type_frame)
                if sub_cls is None:
                    return LoadError('Bad message:'
                            + f' {cls.__module__}.{cls.__qualname__} has no'
                            + f' subclass with key {type_frame!r}')
                return loaders[sub_cls](data_frames)
            case _:
                return LoadError('Bad message: Wrong number of frames')
    return _parse
