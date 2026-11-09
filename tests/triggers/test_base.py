import sys
from datetime import datetime, timedelta
from unittest.mock import Mock

import pytest

from apscheduler.triggers.date import DateTrigger

if sys.version_info < (3, 9):
    from backports.zoneinfo import ZoneInfo
else:
    from zoneinfo import ZoneInfo


@pytest.mark.parametrize(
    "start_date",
    [
        datetime(2026, 10, 25, 2, fold=1),
        datetime(2026, 10, 25, 2, 45),
        datetime(2026, 10, 25, 2, 45, fold=1),
        datetime(2026, 3, 29, 1, 45),
        datetime(2026, 10, 18, 2),
    ],
    ids=["fold", "enter-fold", "leave-fold", "spring-gap", "ordinary-date"],
)
@pytest.mark.parametrize("delay", [0, 0.5, 1800])
def test_jitter_elapsed_time(monkeypatch, start_date, delay):
    timezone = ZoneInfo("Europe/Zagreb")
    start_date = start_date.replace(tzinfo=timezone)
    trigger = DateTrigger(run_date=start_date, timezone=timezone)
    uniform = Mock(return_value=delay)
    monkeypatch.setattr("apscheduler.triggers.base.random.uniform", uniform)

    result = trigger._apply_jitter(start_date, 3600, start_date)
    expected = datetime.fromtimestamp(start_date.timestamp() + delay, timezone)

    assert str(result) == str(expected)
    assert result.fold == expected.fold
    assert result.tzinfo is timezone
    uniform.assert_called_once_with(0, 3600)


def test_jitter_naive_datetime(monkeypatch):
    start_date = datetime(2026, 10, 25, 2)
    trigger = DateTrigger()
    monkeypatch.setattr(
        "apscheduler.triggers.base.random.uniform", Mock(return_value=0.5)
    )

    result = trigger._apply_jitter(start_date, 5, start_date)

    assert result == start_date + timedelta(seconds=0.5)
    assert result.tzinfo is None


@pytest.mark.parametrize(
    "next_fire_time, jitter",
    [
        (None, None),
        (None, 0),
        (None, 5),
        (datetime(2026, 10, 25, 2), None),
        (datetime(2026, 10, 25, 2), 0),
    ],
)
def test_jitter_no_op(monkeypatch, next_fire_time, jitter):
    trigger = DateTrigger()
    uniform = Mock()
    monkeypatch.setattr("apscheduler.triggers.base.random.uniform", uniform)

    assert (
        trigger._apply_jitter(next_fire_time, jitter, datetime.now()) is next_fire_time
    )
    uniform.assert_not_called()
