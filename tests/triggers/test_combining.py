import pickle
from datetime import datetime, timedelta

import pytest

from apscheduler.triggers.combining import AndTrigger, BaseCombiningTrigger, OrTrigger
from apscheduler.triggers.cron import CronTrigger
from apscheduler.util import localize


class TestAndTrigger:
    @pytest.fixture
    def trigger(self, timezone):
        return AndTrigger(
            [
                CronTrigger(
                    month="5-8",
                    day="6-15",
                    end_date=localize(datetime(2017, 8, 10), timezone),
                ),
                CronTrigger(
                    month="6-9",
                    day="*/3",
                    end_date=localize(datetime(2017, 9, 7), timezone),
                ),
            ]
        )

    @pytest.mark.parametrize(
        "start_time, expected",
        [
            (datetime(2017, 8, 6), datetime(2017, 8, 7)),
            (datetime(2017, 8, 10, 1), None),
        ],
        ids=["firstmatch", "end"],
    )
    def test_next_fire_time(self, trigger, timezone, start_time, expected):
        expected = localize(expected, timezone) if expected else None
        assert (
            trigger.get_next_fire_time(None, localize(start_time, timezone)) == expected
        )

    def test_jitter(self, trigger, timezone):
        trigger.jitter = 5
        start_time = localize(datetime(2017, 8, 6), timezone)
        expected = localize(datetime(2017, 8, 7), timezone)
        for _ in range(100):
            next_fire_time = trigger.get_next_fire_time(None, start_time)
            assert abs(expected - next_fire_time) <= timedelta(seconds=5)

    @pytest.mark.parametrize("jitter", [None, 5], ids=["nojitter", "jitter"])
    def test_repr(self, trigger, jitter):
        trigger.jitter = jitter
        jitter_part = f", jitter={jitter}" if jitter else ""
        assert repr(trigger) == (
            "<AndTrigger([<CronTrigger (month='5-8', day='6-15', "
            "end_date='2017-08-10 00:00:00 CEST', timezone='Europe/Berlin')>, <CronTrigger "
            "(month='6-9', day='*/3', end_date='2017-09-07 00:00:00 CEST', "
            f"timezone='Europe/Berlin')>]{jitter_part})>"
        )

    def test_str(self, trigger):
        assert (
            str(trigger)
            == "and[cron[month='5-8', day='6-15'], cron[month='6-9', day='*/3']]"
        )

    @pytest.mark.parametrize("jitter", [None, 5], ids=["nojitter", "jitter"])
    def test_pickle(self, trigger, jitter):
        """Test that the trigger is pickleable."""
        trigger.jitter = jitter
        data = pickle.dumps(trigger, 2)
        trigger2 = pickle.loads(data)

        for attr in BaseCombiningTrigger.__slots__:
            assert repr(getattr(trigger2, attr)) == repr(getattr(trigger, attr))


class TestOrTrigger:
    @pytest.fixture
    def trigger(self, timezone):
        return OrTrigger(
            [
                CronTrigger(
                    month="5-8",
                    day="6-15",
                    end_date=localize(datetime(2017, 8, 10), timezone),
                ),
                CronTrigger(
                    month="6-9",
                    day="*/3",
                    end_date=localize(datetime(2017, 9, 7), timezone),
                ),
            ]
        )

    @pytest.mark.parametrize(
        "start_time, expected",
        [(datetime(2017, 8, 6), datetime(2017, 8, 6)), (datetime(2017, 9, 7, 1), None)],
        ids=["earliest", "end"],
    )
    def test_next_fire_time(self, trigger, timezone, start_time, expected):
        expected = localize(expected, timezone) if expected else None
        assert (
            trigger.get_next_fire_time(None, localize(start_time, timezone)) == expected
        )

    def test_jitter(self, trigger, timezone):
        trigger.jitter = 5
        start_time = expected = localize(datetime(2017, 8, 6), timezone)
        for _ in range(100):
            next_fire_time = trigger.get_next_fire_time(None, start_time)
            assert abs(expected - next_fire_time) <= timedelta(seconds=5)

    @pytest.mark.parametrize("jitter", [None, 5], ids=["nojitter", "jitter"])
    def test_repr(self, trigger, jitter):
        trigger.jitter = jitter
        jitter_part = f", jitter={jitter}" if jitter else ""
        assert repr(trigger) == (
            "<OrTrigger([<CronTrigger (month='5-8', day='6-15', "
            "end_date='2017-08-10 00:00:00 CEST', timezone='Europe/Berlin')>, <CronTrigger "
            "(month='6-9', day='*/3', end_date='2017-09-07 00:00:00 CEST', "
            f"timezone='Europe/Berlin')>]{jitter_part})>"
        )

    def test_str(self, trigger):
        assert (
            str(trigger)
            == "or[cron[month='5-8', day='6-15'], cron[month='6-9', day='*/3']]"
        )

    @pytest.mark.parametrize("jitter", [None, 5], ids=["nojitter", "jitter"])
    def test_pickle(self, trigger, jitter):
        """Test that the trigger is pickleable."""
        trigger.jitter = jitter
        data = pickle.dumps(trigger, 2)
        trigger2 = pickle.loads(data)

        for attr in BaseCombiningTrigger.__slots__:
            assert repr(getattr(trigger2, attr)) == repr(getattr(trigger, attr))
