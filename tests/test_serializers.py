from __future__ import annotations

from datetime import datetime, timezone
from typing import NoReturn
from uuid import uuid4

import pytest

from apscheduler import (
    DeserializationError,
    Event,
    JobAdded,
    JobOutcome,
    JobReleased,
    SerializationError,
)
from apscheduler.abc import Serializer
from apscheduler.triggers.combining import OrTrigger
from apscheduler.triggers.interval import IntervalTrigger


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
                scheduled_start=datetime.now(timezone.utc),
                started_at=datetime.now(timezone.utc),
            ),
            id="job_released",
        ),
    ],
)
def test_serialize_event(event: Event, serializer: Serializer) -> None:
    payload = serializer.serialize(event.marshal())
    deserialized = type(event).unmarshal(serializer.deserialize(payload))
    assert deserialized == event


def test_serialize_trigger_with_mutable_list_state(serializer: Serializer) -> None:
    # Regression test for cbor2 >= 6: a custom object is serialized as a tag and
    # its contents are deserialized as immutable objects, so a list stored in the
    # object's state comes back as a tuple. OrTrigger keeps a mutable list of the
    # next fire times that must survive the round-trip and stay usable.
    start_time = datetime(2020, 1, 1, tzinfo=timezone.utc)

    def make_trigger() -> OrTrigger:
        return OrTrigger(
            triggers=[
                IntervalTrigger(hours=1, start_time=start_time),
                IntervalTrigger(hours=2, start_time=start_time),
            ]
        )

    control = make_trigger()
    trigger = make_trigger()

    # Advance both once so the internal list of fire times is populated, then
    # round-trip the trigger through the serializer.
    assert trigger.next() == control.next()
    trigger = serializer.deserialize(serializer.serialize(trigger))

    # The restored trigger must keep producing the same fire times; this is what
    # raised "'tuple' object does not support item assignment" under cbor2 >= 6.
    for _ in range(3):
        assert trigger.next() == control.next()


def test_serialization_error(serializer: Serializer) -> None:
    class Unserializable:
        def __getstate__(self) -> NoReturn:
            raise ValueError("cannot be serialized")

    # An open file cannot be serialized
    with pytest.raises(SerializationError) as exc:
        serializer.serialize(Unserializable())

    assert isinstance(exc.value.__cause__, ValueError)


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
