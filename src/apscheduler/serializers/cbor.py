from __future__ import annotations

from datetime import date, timedelta, tzinfo
from enum import Enum
from typing import Any

import attrs
from cbor2 import CBOREncoder, CBOREncodeTypeError, CBORTag, dumps, loads

from .. import DeserializationError, SerializationError
from .._marshalling import marshal_object, marshal_timezone, unmarshal_object
from ..abc import Serializer


@attrs.define(kw_only=True, eq=False)
class CBORSerializer(Serializer):
    """
    Serializes objects using CBOR (:rfc:`8949`).

    Can serialize types not normally CBOR serializable, if they implement
    ``__getstate__()`` and ``__setstate__()``.

    :param type_tag: CBOR tag number for indicating arbitrary serialized object
    :param dump_options: keyword arguments passed to :func:`cbor2.dumps`
    :param load_options: keyword arguments passed to :func:`cbor2.loads`
    """

    type_tag: int = 4664
    dump_options: dict[str, Any] = attrs.field(factory=dict)
    load_options: dict[str, Any] = attrs.field(factory=dict)

    def __attrs_post_init__(self) -> None:
        self.dump_options.setdefault("default", self._default_hook)
        self.load_options.setdefault("tag_hook", self._tag_hook)

    def _default_hook(self, encoder: CBOREncoder, value: object) -> None:
        if isinstance(value, date):
            encoder.encode(value.isoformat())
        elif isinstance(value, timedelta):
            encoder.encode(value.total_seconds())
        elif isinstance(value, tzinfo):
            encoder.encode(marshal_timezone(value))
        elif isinstance(value, Enum):
            encoder.encode(value.name)
        elif hasattr(value, "__getstate__"):
            marshalled = marshal_object(value)
            encoder.encode(CBORTag(self.type_tag, marshalled))
        else:
            raise CBOREncodeTypeError(
                f"cannot serialize type {value.__class__.__name__}"
            )

    def _tag_hook(self, *args: Any) -> object:
        # cbor2 < 6 calls the tag hook as (decoder, tag, shareable_index=None),
        # while cbor2 >= 6 calls it as (tag, immutable). Pick out the CBORTag so
        # that deserialization works regardless of the installed cbor2 version.
        tag = next(arg for arg in args if isinstance(arg, CBORTag))
        if tag.tag == self.type_tag:
            cls_ref, state = tag.value
            return unmarshal_object(cls_ref, state)

        return tag

    def serialize(self, obj: object) -> bytes:
        try:
            return dumps(obj, **self.dump_options)
        except Exception as exc:
            raise SerializationError from exc

    def deserialize(self, serialized: bytes):
        try:
            return loads(serialized, **self.load_options)
        except Exception as exc:
            raise DeserializationError from exc
