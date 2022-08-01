from __future__ import annotations

from uuid import uuid4

import pytest

from apscheduler import Event, JobAdded
from apscheduler.abc import Serializer


@pytest.mark.parametrize(
    "event",
    [
        pytest.param(
            JobAdded(
                job_id=uuid4(),
                task_id="task",
                schedule_id="schedule",
                tags=["tag1", "tag2"],
            ),
            id="job_added",
        )
    ],
)
def test_serialize_event(event: Event, serializer: Serializer) -> None:
    payload = serializer.serialize(event.marshal(serializer))
    deserialized = type(event).unmarshal(serializer, serializer.deserialize(payload))
    assert deserialized == event
