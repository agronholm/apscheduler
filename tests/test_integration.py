from time import sleep

import pytest

from apscheduler.scheduler import Scheduler
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_MISSED
from tests.conftest import all_jobstores, all_jobstores_ids


def increment(vals, sleeptime):
    vals[0] += 1
    sleep(sleeptime)


@pytest.fixture(params=all_jobstores, ids=all_jobstores_ids)
def sched(request):
    jobstore = request.param(request)
    sched = Scheduler()
    sched.add_jobstore(jobstore, 'persistent')
    sched.start()
    request.addfinalizer(sched.shutdown)
    return sched


def test_overlapping_runs(sched):
    # Makes sure that "increment" is only ran once, since it will still be
    # running when the next appointed time hits.

    vals = [0]
    sched.add_job(increment, 'interval', {'seconds': 1}, args=[vals, 2], jobstore='persistent')
    sleep(2.5)
    assert vals == [1]


def test_max_instances(sched):
    vals = [0]
    events = []
    sched.add_listener(events.append, EVENT_JOB_EXECUTED | EVENT_JOB_MISSED)
    sched.add_job(increment, 'interval', {'seconds': 0.3}, max_instances=2, max_runs=4, args=[vals, 1],
                  jobstore='persistent')
    sleep(2.4)
    assert vals == [2]
    assert len(events) == 4
    assert events[0].code == EVENT_JOB_MISSED
    assert events[1].code == EVENT_JOB_MISSED
    assert events[2].code == EVENT_JOB_EXECUTED
    assert events[3].code == EVENT_JOB_EXECUTED
