from __future__ import annotations

from uuid import uuid4

from apscheduler import JobOutcome, JobReleased
from apscheduler.abc import Serializer


def test_serialize_job_released(serializer: Serializer) -> None:
    event = JobReleased(
        job_id=uuid4(), worker_id="test_worker", outcome=JobOutcome.success
    )
    payload = serializer.serialize(event.marshal(serializer))
    event2 = JobReleased.unmarshal(serializer, serializer.deserialize(payload))
    assert event == event2
