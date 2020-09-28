from dataclasses import dataclass, field
from json import dumps, loads
from typing import Any, Dict

from ..abc import Serializer
from ..marshalling import marshal_object, unmarshal_object


@dataclass
class JSONSerializer(Serializer):
    magic_key: str = '_apscheduler_json'
    dump_options: Dict[str, Any] = field(default_factory=dict)
    load_options: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        self.dump_options['default'] = self._default_hook
        self.load_options['object_hook'] = self._object_hook

    @classmethod
    def _default_hook(cls, obj):
        if hasattr(obj, '__getstate__'):
            cls_ref, state = marshal_object(obj)
            return {cls.magic_key: [cls_ref, state]}

        raise TypeError(f'Object of type {obj.__class__.__name__!r} is not JSON serializable')

    @classmethod
    def _object_hook(cls, obj_state: Dict[str, Any]):
        if cls.magic_key in obj_state:
            ref, *rest = obj_state[cls.magic_key]
            return unmarshal_object(ref, *rest)

        return obj_state

    def serialize(self, obj) -> bytes:
        return dumps(obj, ensure_ascii=False, **self.dump_options).encode('utf-8')

    def deserialize(self, serialized: bytes):
        return loads(serialized, **self.load_options)

    def serialize_to_unicode(self, obj) -> str:
        return dumps(obj, ensure_ascii=False, **self.dump_options)

    def deserialize_from_unicode(self, serialized: str):
        return loads(serialized, **self.load_options)
