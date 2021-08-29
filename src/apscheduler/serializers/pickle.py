from dataclasses import dataclass
from pickle import dumps, loads

from ..abc import Serializer


@dataclass(frozen=True)
class PickleSerializer(Serializer):
    protocol: int = 4

    def serialize(self, obj) -> bytes:
        return dumps(obj, self.protocol)

    def deserialize(self, serialized: bytes):
        return loads(serialized)
