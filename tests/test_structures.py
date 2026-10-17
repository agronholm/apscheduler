from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest

from apscheduler import DeserializationError, Job, Schedule, SerializationError
from apscheduler.abc import Serializer
from apscheduler.serializers.pickle import PickleSerializer
from apscheduler.triggers.date import DateTrigger


def make_invocation(kind: str, **kwargs: Any) -> Job | Schedule:
    if kind == "job":
        return Job(task_id="shared", executor="async", **kwargs)

    return Schedule(
        id="schedule",
        task_id="shared",
        trigger=DateTrigger(datetime.now(UTC)),
        job_executor="async",
        **kwargs,
    )


@pytest.mark.parametrize("kind", ["job", "schedule"])
@pytest.mark.parametrize("func", [None, "some_module:Child.run"])
def test_invocation_roundtrip(
    kind: str, func: str | None, serializer: Serializer
) -> None:
    # A marker-looking mapping inside user args is never an envelope. Neither
    # class-looking strings nor other positional arguments are reinterpreted.
    args = (
        {"apscheduler:callable": 1, "func": "user:data", "args": []},
        "some_module:Parent",
        "user argument",
    )
    invocation = make_invocation(kind, args=args, kwargs={"func": "user"}, func=func)
    marshalled = invocation.marshal(serializer)
    ordinary = make_invocation(kind).marshal(serializer)
    assert marshalled.keys() == ordinary.keys()
    assert "_func" not in marshalled
    assert "func" not in marshalled
    if func is None:
        assert marshalled["args"] == serializer.serialize(args)
    else:
        envelope = serializer.deserialize(marshalled["args"])
        assert envelope["apscheduler:callable"] == 1
        assert envelope["func"] == func
        assert tuple(envelope["args"]) == args

    restored = type(invocation).unmarshal(serializer, marshalled)
    assert restored._func == func
    assert restored.args == args
    assert restored.kwargs == {"func": "user"}
    assert restored.task_id == "shared"


@pytest.mark.parametrize("kind", ["job", "schedule"])
@pytest.mark.parametrize(
    "envelope",
    [
        {},
        {"apscheduler:callable": 2, "func": "module:run", "args": []},
        {"apscheduler:callable": True, "func": "module:run", "args": []},
        {"apscheduler:callable": 1, "func": None, "args": []},
        {"apscheduler:callable": 1, "func": "module:run", "args": {}},
        {"apscheduler:callable": 1, "func": "module:run"},
        {"apscheduler:callable": 1, "func": "module:run", "args": [], "extra": 0},
    ],
)
def test_invalid_invocation_envelope(
    kind: str, envelope: dict, serializer: Serializer
) -> None:
    invocation = make_invocation(kind)
    marshalled = invocation.marshal(serializer)
    marshalled["args"] = serializer.serialize(envelope)
    with pytest.raises(DeserializationError, match="invocation envelope"):
        type(invocation).unmarshal(serializer, marshalled)


@pytest.mark.parametrize("kind", ["job", "schedule"])
def test_local_invocation_cannot_be_persisted(
    kind: str, serializer: Serializer
) -> None:
    invocation = make_invocation(kind, func=lambda: None)
    with pytest.raises(SerializationError, match="locally defined class method"):
        invocation.marshal(serializer)


@pytest.mark.parametrize("kind", ["job", "schedule"])
def test_base_era_class_argument_is_unchanged(kind: str) -> None:
    # Pickle could persist the injected cls used by base-era submissions. Loading
    # must retain that exact argument, even when a fresh worker cannot call it.
    serializer = PickleSerializer()
    invocation = make_invocation(kind, args=(DateTrigger, "user argument"))
    marshalled = invocation.marshal(serializer)
    assert marshalled["args"] == serializer.serialize(invocation.args)
    restored = type(invocation).unmarshal(serializer, marshalled)
    assert restored._func is None
    assert restored.args == (DateTrigger, "user argument")
