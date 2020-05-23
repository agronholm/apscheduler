from dataclasses import dataclass, field
from typing import Dict, Any

from cbor2 import dumps, loads, CBOREncodeTypeError, CBORTag

from ..abc import Serializer
from ..util import marshal_object, unmarshal_object


@dataclass
class CBORSerializer(Serializer):
    type_tag: int = 4664
    dump_options: Dict[str, Any] = field(default_factory=dict)
    load_options: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        self.dump_options.setdefault('default', self._default_hook)
        self.load_options.setdefault('tag_hook', self._tag_hook)

    def _default_hook(self, encoder, value):
        if hasattr(value, '__getstate__'):
            marshalled = marshal_object(value)
            encoder.encode(CBORTag(self.type_tag, marshalled))
        else:
            raise CBOREncodeTypeError(f'cannot serialize type {value.__class__.__name__}')

    def _tag_hook(self, decoder, tag: CBORTag, shareable_index: int = None):
        if tag.tag == self.type_tag:
            cls_ref, state = tag.value
            return unmarshal_object(cls_ref, state)

    def serialize(self, obj) -> bytes:
        return dumps(obj, **self.dump_options)

    def deserialize(self, serialized: bytes):
        return loads(serialized, **self.load_options)
