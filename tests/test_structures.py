from __future__ import annotations

from datetime import UTC, datetime
from functools import partial

import pytest

from apscheduler import Job, Schedule, Task, TaskDefaults
from apscheduler.serializers.json import JSONSerializer
from apscheduler.triggers.date import DateTrigger


@pytest.fixture(
    params=[
        pytest.param(TaskDefaults, id="task_defaults"),
        pytest.param(
            partial(Task, id="task", func=None, job_executor="threadpool"), id="task"
        ),
        pytest.param(
            partial(
                Schedule,
                id="schedule",
                task_id="task",
                trigger=DateTrigger(datetime(2026, 1, 1, tzinfo=UTC)),
                job_executor="threadpool",
            ),
            id="schedule",
        ),
        pytest.param(partial(Job, task_id="task", executor="threadpool"), id="job"),
    ]
)
def metadata_factory(request):
    return request.param


@pytest.mark.parametrize("key", [1, False, None, ("key",)])
def test_metadata_non_string_key(metadata_factory, key):
    with pytest.raises(ValueError) as exc:
        metadata_factory(metadata={key: "value", "1": "string"})

    assert str(exc.value) == f"metadata has a non-string key ({key!r})"


@pytest.mark.parametrize("key", [1, False, None, ("key",)])
def test_metadata_non_string_key_assignment(metadata_factory, key):
    obj = metadata_factory(metadata={"original": "value"})
    with pytest.raises(ValueError) as exc:
        obj.metadata = {key: "value"}

    assert str(exc.value) == f"metadata has a non-string key ({key!r})"
    assert obj.metadata == {"original": "value"}


def test_metadata_string_keys(metadata_factory):
    metadata = {
        "1": "string",
        "nested": {"values": [None, 1, 1.5, True, "value"]},
        "empty": {},
    }
    obj = metadata_factory(metadata=metadata)
    serializer = JSONSerializer()
    assert serializer.deserialize(serializer.serialize(obj.metadata)) == metadata
