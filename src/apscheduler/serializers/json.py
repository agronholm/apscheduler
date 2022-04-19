from __future__ import annotations

from datetime import datetime
from json import dumps, loads
from typing import Any

import attrs

from ..abc import Serializer
from ..marshalling import marshal_date, marshal_object, unmarshal_object


@attrs.define(kw_only=True, eq=False)
class JSONSerializer(Serializer):
    magic_key: str = "_apscheduler_json"
    dump_options: dict[str, Any] = attrs.field(factory=dict)
    load_options: dict[str, Any] = attrs.field(factory=dict)

    def __attrs_post_init__(self):
        self.dump_options["default"] = self._default_hook
        self.load_options["object_hook"] = self._object_hook

    def _default_hook(self, obj):
        if hasattr(obj, "__getstate__"):
            cls_ref, state = marshal_object(obj)
            return {self.magic_key: [cls_ref, state]}
        elif isinstance(obj, datetime):
            return marshal_date(obj)

        raise TypeError(
            f"Object of type {obj.__class__.__name__!r} is not JSON serializable"
        )

    def _object_hook(self, obj_state: dict[str, Any]):
        if self.magic_key in obj_state:
            ref, *rest = obj_state[self.magic_key]
            return unmarshal_object(ref, *rest)

        return obj_state

    def serialize(self, obj) -> bytes:
        return dumps(obj, ensure_ascii=False, **self.dump_options).encode("utf-8")

    def deserialize(self, serialized: bytes):
        return loads(serialized, **self.load_options)

    def serialize_to_unicode(self, obj) -> str:
        return dumps(obj, ensure_ascii=False, **self.dump_options)

    def deserialize_from_unicode(self, serialized: str):
        return loads(serialized, **self.load_options)
