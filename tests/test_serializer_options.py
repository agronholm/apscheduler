from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest

from apscheduler.serializers.cbor import CBORSerializer
from apscheduler.serializers.json import JSONSerializer
from apscheduler.triggers.interval import IntervalTrigger


@pytest.mark.parametrize("serializer_class", [JSONSerializer, CBORSerializer])
@pytest.mark.parametrize("shared", ["dump", "load", "both"])
def test_reused_options(serializer_class, shared: str) -> None:
    dump_options: dict[str, Any] = {}
    load_options: dict[str, Any] = {}
    kwargs: dict[str, Any] = {}
    if shared in ("dump", "both"):
        kwargs["dump_options"] = dump_options
    if shared in ("load", "both"):
        kwargs["load_options"] = load_options

    configs: list[dict[str, Any]] = (
        [{"magic_key": "first"}, {"magic_key": "second"}]
        if serializer_class is JSONSerializer
        else [{"type_tag": 4664}, {"type_tag": 4665}]
    )
    serializers = [serializer_class(**config, **kwargs) for config in configs]
    for serializer, config in zip(serializers, configs, strict=True):
        fresh = serializer_class(**config)
        trigger = IntervalTrigger(
            seconds=1, start_time=datetime(2026, 1, 1, tzinfo=UTC)
        )
        trigger.next()
        trigger.next()
        restored = fresh.deserialize(serializer.serialize(trigger))
        assert isinstance(restored, IntervalTrigger)
        restored_existing = serializer.deserialize(fresh.serialize(trigger))
        assert isinstance(restored_existing, IntervalTrigger)
        expected = [trigger.next() for _ in range(4)]
        assert [restored.next() for _ in range(4)] == expected
        assert [restored_existing.next() for _ in range(4)] == expected

    assert dump_options == {}
    assert load_options == {}
