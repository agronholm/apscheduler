from datetime import datetime, timedelta, timezone

import pytest
from apscheduler.abc import Schedule
from apscheduler.datastores.memory import MemoryScheduleStore
from apscheduler.events import SchedulesAdded, SchedulesRemoved, SchedulesUpdated
from apscheduler.triggers.date import DateTrigger

pytestmark = pytest.mark.anyio


@pytest.fixture
async def store():
    async with MemoryScheduleStore() as store_:
        yield store_


@pytest.fixture
async def events(store):
    events = []
    await store.subscribe(events.append)
    return events


@pytest.fixture
def schedules():
    trigger = DateTrigger(datetime(2020, 9, 13, tzinfo=timezone.utc))
    schedule1 = Schedule(id='s1', task_id='bogus', trigger=trigger, args=(), kwargs={},
                         coalesce=False, misfire_grace_time=None, tags=frozenset())
    schedule1.next_fire_time = trigger.next()

    trigger = DateTrigger(datetime(2020, 9, 14, tzinfo=timezone.utc))
    schedule2 = Schedule(id='s2', task_id='bogus', trigger=trigger, args=(), kwargs={},
                         coalesce=False, misfire_grace_time=None, tags=frozenset())
    schedule2.next_fire_time = trigger.next()

    trigger = DateTrigger(datetime(2020, 9, 15, tzinfo=timezone.utc))
    schedule3 = Schedule(id='s3', task_id='bogus', trigger=trigger, args=(), kwargs={},
                         coalesce=False, misfire_grace_time=None, tags=frozenset())
    return [schedule1, schedule2, schedule3]


async def test_add_schedules(store, schedules, events):
    assert await store.get_next_fire_time() is None
    await store.add_or_replace_schedules(schedules)
    assert await store.get_next_fire_time() == datetime(2020, 9, 13, tzinfo=timezone.utc)

    assert await store.get_schedules() == schedules
    assert await store.get_schedules({'s1', 's2', 's3'}) == schedules
    assert await store.get_schedules({'s1'}) == [schedules[0]]
    assert await store.get_schedules({'s2'}) == [schedules[1]]
    assert await store.get_schedules({'s3'}) == [schedules[2]]

    assert len(events) == 1
    assert isinstance(events[0], SchedulesAdded)
    assert events[0].schedule_ids == {'s1', 's2', 's3'}
    assert events[0].earliest_next_fire_time == datetime(2020, 9, 13, tzinfo=timezone.utc)


async def test_replace_schedules(store, schedules, events):
    await store.add_or_replace_schedules(schedules)
    events.clear()

    next_fire_time = schedules[2].trigger.next()
    schedule = Schedule(id='s3', task_id='foo', trigger=schedules[2].trigger, args=(), kwargs={},
                        coalesce=False, misfire_grace_time=None, tags=frozenset())
    schedule.next_fire_time = next_fire_time
    await store.add_or_replace_schedules([schedule])

    schedules = await store.get_schedules([schedule.id])
    assert schedules[0].task_id == 'foo'
    assert schedules[0].next_fire_time == next_fire_time
    assert schedules[0].args == ()
    assert schedules[0].kwargs == {}
    assert not schedules[0].coalesce
    assert schedules[0].misfire_grace_time is None
    assert schedules[0].tags == frozenset()

    assert len(events) == 1
    assert isinstance(events[0], SchedulesUpdated)
    assert events[0].schedule_ids == {'s3'}
    assert events[0].earliest_next_fire_time == datetime(2020, 9, 15, tzinfo=timezone.utc)


async def test_update_schedules(store, schedules, events):
    await store.add_or_replace_schedules(schedules)
    events.clear()

    next_fire_time = datetime(2020, 9, 12, tzinfo=timezone.utc)
    updated_ids = await store.update_schedules({
        's2': {'task_id': 'foo', 'next_fire_time': next_fire_time, 'args': (1,),
               'kwargs': {'a': 'x'}, 'coalesce': True, 'misfire_grace_time': timedelta(seconds=5),
               'tags': frozenset(['a', 'b'])},
        'nonexistent': {}
    })
    assert updated_ids == {'s2'}

    schedules = await store.get_schedules({'s2'})
    assert len(schedules) == 1
    assert schedules[0].task_id == 'foo'
    assert schedules[0].args == (1,)
    assert schedules[0].kwargs == {'a': 'x'}
    assert schedules[0].coalesce
    assert schedules[0].next_fire_time == next_fire_time
    assert schedules[0].misfire_grace_time == timedelta(seconds=5)
    assert schedules[0].tags == frozenset(['a', 'b'])

    assert len(events) == 1
    assert isinstance(events[0], SchedulesUpdated)
    assert events[0].schedule_ids == {'s2'}
    assert events[0].earliest_next_fire_time == next_fire_time


@pytest.mark.parametrize('ids', [None, {'s1', 's2'}], ids=['all', 'by_id'])
async def test_remove_schedules(store, schedules, events, ids):
    await store.add_or_replace_schedules(schedules)
    events.clear()

    await store.remove_schedules(ids)
    assert len(events) == 1
    assert isinstance(events[0], SchedulesRemoved)
    if ids:
        assert events[0].schedule_ids == {'s1', 's2'}
        assert await store.get_schedules() == [schedules[2]]
    else:
        assert events[0].schedule_ids == {'s1', 's2', 's3'}
        assert await store.get_schedules() == []


async def test_acquire_due_schedules(store, schedules, events):
    await store.add_or_replace_schedules(schedules)
    events.clear()

    now = datetime(2020, 9, 14, tzinfo=timezone.utc)
    async with store.acquire_due_schedules('dummy-id', now) as schedules:
        assert len(schedules) == 2
        assert schedules[0].id == 's1'
        assert schedules[1].id == 's2'
