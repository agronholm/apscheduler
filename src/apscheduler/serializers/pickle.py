from __future__ import annotations

from pickle import dumps, loads

import attrs

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

    def serialize(self, obj) -> bytes:
        return dumps(obj, self.protocol)

    def deserialize(self, serialized: bytes):
        return loads(serialized)
