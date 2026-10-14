from __future__ import annotations

import json

import pytest

from apscheduler.serializers.json import JSONSerializer


@pytest.mark.parametrize("ensure_ascii", [True, False])
def test_explicit_ensure_ascii(ensure_ascii: bool) -> None:
    payload = {"label": "測定 café 🎵", "samples": [1, 2, 3]}
    serializer = JSONSerializer(
        dump_options={"ensure_ascii": ensure_ascii, "sort_keys": True}
    )
    serialized = serializer.serialize(payload)
    assert serialized == json.dumps(
        payload, ensure_ascii=ensure_ascii, sort_keys=True
    ).encode("utf-8")
    assert serializer.deserialize(serialized) == payload
