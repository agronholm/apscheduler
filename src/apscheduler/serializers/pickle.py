from __future__ import annotations

from pickle import dumps, loads

import attrs

from .. import DeserializationError, SerializationError
from ..abc import Serializer


@attrs.define(kw_only=True, eq=False)
class PickleSerializer(Serializer):
    """
    Uses the :mod:`pickle` module to (de)serialize objects.

    As this serialization method is native to Python, it is able to serialize a wide
    range of types, at the expense of being insecure. Do **not** use this serializer
    unless you can fully trust the entire system to not have maliciously injected data.
    Such data can be made to call arbitrary functions with arbitrary arguments on
    unpickling.

    :param protocol: the pickle protocol number to use
    """

    protocol: int = 4

    def serialize(self, obj: object) -> bytes:
        try:
            return dumps(obj, self.protocol)
        except Exception as exc:
            raise SerializationError from exc

    def deserialize(self, serialized: bytes):
        try:
            return loads(serialized)
        except Exception as exc:
            raise DeserializationError from exc
