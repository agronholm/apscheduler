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
