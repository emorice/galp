"""
Tests for pack
"""
from dataclasses import dataclass
from typing import Optional
import msgpack
import pytest

from galp.pack import dump, load, Payload, LoadError, Ok

@dataclass(frozen=True)
class A:
    """simple dataclass"""
    x: int

@dataclass(frozen=True)
class B:
    """nested dataclass"""
    x: float
    a: A
    z: str

@dataclass(frozen=True)
class C:
    """basic type payload"""
    x: int
    y: Payload[bytes]
    z: float

@dataclass(frozen=True)
class D:
    """dataclass payload"""
    x: int
    y: Payload[A]
    z: float

@dataclass(frozen=True)
class E:
    """members with payload"""
    x: C # inline member with a payload
    y: Payload[bytes] # payload with basic type
    z: Payload[C] # payload member with a payload
    a: str

@dataclass(frozen=True)
class Uni:
    """member union"""
    mem: A | B

@dataclass(frozen=True)
class UniPayload:
    """payload member union with a payload"""
    mem: Payload[A | D]
    l: list

@dataclass(frozen=True)
class ListLikes:
    """list and tuple of dataclasses member"""
    l: list[list[C]]
    t: tuple[C, ...]

@dataclass(frozen=True)
class Dict:
    """dict of dataclasses member"""
    d: dict[str, C]

@dataclass(frozen=True)
class Opt:
    """option"""
    x: Optional[A]

@dataclass(frozen=True)
class HasDefault:
    """option"""
    x: int
    y: int = 0

@dataclass
class Document:
    """Arbitrary msgpack"""
    x: object

@pytest.mark.parametrize('case', [
    (A(1), 1),
    (B(1.5, A(1), 'bar'), 1),
    (C(1, b'blorbo', 2.5), 2),
    (D(1, A(3), 2.5), 2),
    (E(C(1, b'blorbo', 2.5), b'foo', C(2, b'bar', 3.3), 'wow'), 5),
    (Uni(A(2)), 1),
    (UniPayload(D(1, A(3), 2.5), [2]), 3),
    (ListLikes(
        [[C(1, b'blorbo', 2.5), C(2, b'blirbi', 3.4)]],
        (C(3, b'blurbu', 4.5), C(4, b'blarba', 6.4)),
        ), 5),
    (Dict({'key': C(1, b'blorbo', 2.5), 'cokey': C(2, b'blirbi', 3.4)}), 3),
    (Opt(A(1)), 1),
    (Opt(None), 1),
    (Document({'a': 1, 'b': [b'xoxo', 'abc']}), 1),
    ])
def test_pack_unpack(case):
    """
    Objects can be serialized and reloaded
    """
    obj, n_frames = case
    msg = dump(obj)
    assert all(isinstance(f, bytes) for f in msg)
    assert len(msg) == n_frames
    print(msgpack.loads(msg[0]), flush=True)
    objback = load(type(obj), msg).unwrap()
    assert obj == objback

def test_failure():
    """
    Correctly return LoadError on failed validation
    """

    frames = [msgpack.dumps({})]
    obj = load(A, frames)
    assert isinstance(obj, LoadError)

def test_default():
    """
    Handle objects where an attribute with a default is missing from the
    serialized data. Important to extend object models.
    """
    frames = [msgpack.dumps({'x': 1})] # No y
    obj = load(HasDefault, frames)
    assert obj == Ok(HasDefault(x=1, y=0))
