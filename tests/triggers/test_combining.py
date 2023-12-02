from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from apscheduler import MaxIterationsReached
from apscheduler.triggers.combining import AndTrigger, OrTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger


class TestAndTrigger:
    @pytest.mark.parametrize("threshold", [1, 0])
    def test_two_datetriggers(self, timezone, serializer, threshold):
        date1 = datetime(2020, 5, 16, 14, 17, 30, 254212, tzinfo=timezone)
        date2 = datetime(2020, 5, 16, 14, 17, 31, 254212, tzinfo=timezone)
        trigger = AndTrigger(
            [DateTrigger(date1), DateTrigger(date2)], threshold=threshold
        )
        if serializer:
            trigger = serializer.deserialize(serializer.serialize(trigger))

        if threshold:
            # date2 was within the threshold so it will not be used
            assert trigger.next() == date1

        assert trigger.next() is None

    def test_max_iterations(self, timezone, serializer):
        start_time = datetime(2020, 5, 16, 14, 17, 30, 254212, tzinfo=timezone)
        trigger = AndTrigger(
            [
                IntervalTrigger(seconds=4, start_time=start_time),
                IntervalTrigger(
                    seconds=4, start_time=start_time + timedelta(seconds=2)
                ),
            ]
        )
        if serializer:
            trigger = serializer.deserialize(serializer.serialize(trigger))

        pytest.raises(MaxIterationsReached, trigger.next)

    def test_repr(self, timezone, serializer):
        start_time = datetime(2020, 5, 16, 14, 17, 30, 254212, tzinfo=timezone)
        trigger = AndTrigger(
            [
                IntervalTrigger(seconds=4, start_time=start_time),
                IntervalTrigger(
                    seconds=4, start_time=start_time + timedelta(seconds=2)
                ),
            ]
        )
        if serializer:
            trigger = serializer.deserialize(serializer.serialize(trigger))

        assert repr(trigger) == (
            "AndTrigger([IntervalTrigger(seconds=4, "
            "start_time='2020-05-16 14:17:30.254212+02:00'), IntervalTrigger("
            "seconds=4, start_time='2020-05-16 14:17:32.254212+02:00')], "
            "threshold=1.0, max_iterations=10000)"
        )


class TestOrTrigger:
    def test_two_datetriggers(self, timezone, serializer):
        date1 = datetime(2020, 5, 16, 14, 17, 30, 254212, tzinfo=timezone)
        date2 = datetime(2020, 5, 18, 15, 1, 53, 940564, tzinfo=timezone)
        trigger = OrTrigger([DateTrigger(date1), DateTrigger(date2)])

        assert trigger.next() == date1

        if serializer:
            trigger = serializer.deserialize(serializer.serialize(trigger))

        assert trigger.next() == date2
        assert trigger.next() is None

    def test_two_interval_triggers(self, timezone, serializer):
        start_time = datetime(2020, 5, 16, 14, 17, 30, 254212, tzinfo=timezone)
        end_time1 = start_time + timedelta(seconds=16)
        end_time2 = start_time + timedelta(seconds=18)
        trigger = OrTrigger(
            [
                IntervalTrigger(seconds=4, start_time=start_time, end_time=end_time1),
                IntervalTrigger(seconds=6, start_time=start_time, end_time=end_time2),
            ]
        )
        if serializer:
            trigger = serializer.deserialize(serializer.serialize(trigger))

        assert trigger.next() == start_time
        assert trigger.next() == start_time + timedelta(seconds=4)
        assert trigger.next() == start_time + timedelta(seconds=6)
        assert trigger.next() == start_time + timedelta(seconds=8)
        assert trigger.next() == start_time + timedelta(seconds=12)
        assert trigger.next() == start_time + timedelta(seconds=16)
        # The end time of the 4 second interval has been reached
        assert trigger.next() == start_time + timedelta(seconds=18)
        # The end time of the 6 second interval has been reached
        assert trigger.next() is None

    def test_repr(self, timezone):
        date1 = datetime(2020, 5, 16, 14, 17, 30, 254212, tzinfo=timezone)
        date2 = datetime(2020, 5, 18, 15, 1, 53, 940564, tzinfo=timezone)
        trigger = OrTrigger([DateTrigger(date1), DateTrigger(date2)])
        assert repr(trigger) == (
            "OrTrigger([DateTrigger('2020-05-16 14:17:30.254212+02:00'), "
            "DateTrigger('2020-05-18 15:01:53.940564+02:00')])"
        )
