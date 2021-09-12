from __future__ import annotations

from datetime import datetime

from apscheduler.triggers.date import DateTrigger


def test_run_time(timezone, serializer):
    run_time = datetime(2020, 5, 14, 11, 56, 12, tzinfo=timezone)
    trigger = DateTrigger(run_time)
    if serializer:
        payload = serializer.serialize(trigger)
        trigger = serializer.deserialize(payload)

    assert trigger.next() == run_time
    assert trigger.next() is None
    assert repr(trigger) == "DateTrigger('2020-05-14 11:56:12+02:00')"
