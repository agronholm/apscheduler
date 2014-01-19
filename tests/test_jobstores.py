from datetime import datetime

from dateutil.tz import tzoffset
import pytest

from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.jobstores.base import JobStore
from apscheduler.triggers.date import DateTrigger
from apscheduler.job import Job
from tests.conftest import persistent_jobstores, all_jobstores, all_jobstores_ids, persistent_jobstores_ids

local_tz = tzoffset('DUMMYTZ', 3600)


def dummy_job():
    pass


def dummy_job2():
    pass


def dummy_job3():
    pass


@pytest.mark.parametrize('jobstore', all_jobstores, indirect=True, ids=all_jobstores_ids)
def test_jobstore_add_update_remove(jobstore):
    trigger_date = datetime(2999, 1, 1)
    trigger = DateTrigger({}, trigger_date, local_tz)
    job = Job(trigger, dummy_job, [], {}, 1, False, None, None, 1)
    job.next_run_time = trigger_date

    assert jobstore.jobs == []

    jobstore.add_job(job)
    assert jobstore.jobs == [job]
    assert jobstore.jobs[0] == job
    assert jobstore.jobs[0].runs == 0

    job.runs += 1
    jobstore.update_job(job)
    jobstore.load_jobs()
    assert len(jobstore.jobs) == 1
    assert jobstore.jobs == [job]
    assert jobstore.jobs[0].runs == 1

    jobstore.remove_job(job)
    assert jobstore.jobs == []
    jobstore.load_jobs()
    assert jobstore.jobs == []


@pytest.mark.parametrize('jobstore', persistent_jobstores, indirect=True, ids=persistent_jobstores_ids)
def test_one_job_fails_to_load(jobstore):
    trigger_date = datetime(2999, 1, 1)
    trigger = DateTrigger({}, trigger_date, local_tz)

    global dummy_job2, dummy_job_temp
    job1 = Job(trigger, dummy_job, [], {}, 1, False, None, None, 1)
    job2 = Job(trigger, dummy_job2, [], {}, 1, False, None, None, 1)
    job3 = Job(trigger, dummy_job3, [], {}, 1, False, None, None, 1)
    for job in job1, job2, job3:
        job.next_run_time = trigger_date
        jobstore.add_job(job)

    dummy_job_temp = dummy_job2
    del dummy_job2
    try:
        jobstore.load_jobs()
        assert len(jobstore.jobs) == 2
    finally:
        dummy_job2 = dummy_job_temp
        del dummy_job_temp


def test_repr_memjobstore(memjobstore):
    assert repr(memjobstore) == '<MemoryJobStore>'


def test_repr_shelvejobstore(shelvejobstore):
    assert repr(shelvejobstore) == '<ShelveJobStore (path=%s)>' % shelvejobstore.path


def test_repr(sqlalchemyjobstore):
    assert repr(sqlalchemyjobstore) == '<SQLAlchemyJobStore (url=sqlite:///)>'


def test_repr(mongodbjobstore):
    assert repr(mongodbjobstore) == "<MongoDBJobStore (connection=Connection('localhost', 27017))>"


def test_repr_redisjobstore(redisjobstore):
    assert repr(redisjobstore) == "<RedisJobStore>"


def test_sqlalchemy_invalid_args():
    if not SQLAlchemyJobStore:
        pytest.skip('SQLAlchemyJobStore missing')

    pytest.raises(ValueError, SQLAlchemyJobStore)


def test_sqlalchemy_alternate_tablename():
    if not SQLAlchemyJobStore:
        pytest.skip('SQLAlchemyJobStore missing')

    store = SQLAlchemyJobStore('sqlite:///', tablename='test_table')
    assert store.jobs_t.name == 'test_table'


def test_unimplemented_job_store():
    store = JobStore()
    pytest.raises(NotImplementedError, store.add_job, None)
    pytest.raises(NotImplementedError, store.update_job, None)
    pytest.raises(NotImplementedError, store.remove_job, None)
    pytest.raises(NotImplementedError, store.load_jobs)
