from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone, tzinfo
from typing import NoReturn
from uuid import uuid4
from zoneinfo import ZoneInfo

import pytest

from apscheduler import (
    DeserializationError,
    Event,
    JobAdded,
    JobOutcome,
    JobReleased,
    SerializationError,
)
from apscheduler._converters import as_timezone
from apscheduler._marshalling import unmarshal_timezone
from apscheduler.abc import Serializer
from apscheduler.serializers.cbor import CBORSerializer
from apscheduler.serializers.json import JSONSerializer


@pytest.mark.parametrize(
    "serializer_class", [JSONSerializer, CBORSerializer], ids=["json", "cbor"]
)
@pytest.mark.parametrize(
    ("tz_value", "expected_name", "expected_offset"),
    [
        pytest.param(UTC, "UTC", timedelta(0), id="utc"),
        pytest.param(
            timezone(timedelta(hours=2)), "UTC+02:00", timedelta(hours=2), id="fixed+2"
        ),
        pytest.param(
            timezone(timedelta(hours=-5, minutes=-30)),
            "UTC-05:30",
            timedelta(hours=-5, minutes=-30),
            id="fixed-5:30",
        ),
    ],
)
def test_serialize_builtin_timezone(
    serializer_class, tz_value: timezone, expected_name: str, expected_offset: timedelta
) -> None:
    serializer = serializer_class()
    name = serializer.deserialize(serializer.serialize(tz_value))
    assert name == expected_name

    restored = unmarshal_timezone(name)
    assert isinstance(restored, tzinfo)
    assert restored.utcoffset(None) == expected_offset


@pytest.mark.parametrize(
    "serializer_class", [JSONSerializer, CBORSerializer], ids=["json", "cbor"]
)
def test_serialize_zoneinfo_timezone_unchanged(serializer_class) -> None:
    serializer = serializer_class()
    name = serializer.deserialize(serializer.serialize(ZoneInfo("Europe/Berlin")))
    assert name == "Europe/Berlin"


@pytest.mark.parametrize(
    "event",
    [
        pytest.param(
            JobAdded(
                job_id=uuid4(),
                task_id="task",
                schedule_id="schedule",
            ),
            id="job_added",
        ),
        pytest.param(
            JobReleased(
                job_id=uuid4(),
                scheduler_id="testscheduler",
                task_id="task",
                schedule_id="schedule",
                outcome=JobOutcome.success,
                scheduled_start=datetime.now(UTC),
                started_at=datetime.now(UTC),
            ),
            id="job_released",
        ),
    ],
)
def test_serialize_event(event: Event, serializer: Serializer) -> None:
    payload = serializer.serialize(event.marshal())
    deserialized = type(event).unmarshal(serializer.deserialize(payload))
    assert deserialized == event


def test_serialization_error(serializer: Serializer) -> None:
    class Unserializable:
        def __getstate__(self) -> NoReturn:
            raise ValueError("cannot be serialized")

    # An open file cannot be serialized
    with pytest.raises(SerializationError) as exc:
        serializer.serialize(Unserializable())

    assert isinstance(exc.value.__cause__, ValueError)


@pytest.mark.parametrize(
    "serializer_class", [JSONSerializer, CBORSerializer], ids=["json", "cbor"]
)
@pytest.mark.parametrize(
    "tz_value",
    [
        UTC,
        timezone(timedelta(hours=2)),
        timezone(timedelta(hours=-5, minutes=-30)),
        ZoneInfo("Europe/Berlin"),
    ],
    ids=["utc", "fixed+2", "fixed-5:30", "zoneinfo"],
)
def test_timezone_roundtrip_through_as_timezone(serializer_class, tz_value) -> None:
    # Triggers feed the serialized timezone name through as_timezone(); the
    # marshalled fixed-offset form must survive that step too.
    serializer = serializer_class()
    name = serializer.deserialize(serializer.serialize(tz_value))

    restored = as_timezone(name)
    assert restored.utcoffset(None) == tz_value.utcoffset(None)


@pytest.mark.parametrize(
    "payload",
    [
        pytest.param(b"", id="empty"),
        pytest.param(b"\x61\x98", id="invalid"),
    ],
)
def test_deserialization_error(payload: bytes, serializer: Serializer) -> None:
    with pytest.raises(DeserializationError) as exc:
        serializer.deserialize(payload)

    assert exc.value.__cause__ is not None
