from pickle import dumps, loads

import attrs

from ..abc import Serializer


@attrs.define(kw_only=True, eq=False)
class PickleSerializer(Serializer):
    protocol: int = 4

    def serialize(self, obj) -> bytes:
        return dumps(obj, self.protocol)

    def deserialize(self, serialized: bytes):
        return loads(serialized)
