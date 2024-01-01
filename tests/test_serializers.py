from __future__ import annotations

from uuid import uuid4

import pytest

from apscheduler import Event, JobAdded, JobOutcome, JobReleased
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
            ),
            id="job_released",
        ),
    ],
)
def test_serialize_event(event: Event, serializer: Serializer) -> None:
    payload = serializer.serialize(event.marshal())
    deserialized = type(event).unmarshal(serializer.deserialize(payload))
    assert deserialized == event
