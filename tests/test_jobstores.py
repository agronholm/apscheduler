from datetime import datetime
import os

from dateutil.tz import tzoffset
import pytest

from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.jobstores.base import JobLookupError, ConflictingIdError, TransientJobError
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.date import DateTrigger
from apscheduler.job import Job

local_tz = tzoffset('DUMMYTZ', 3600)


def dummy_job():
    pass


def dummy_job2():
    pass


def dummy_job3():
    pass


@pytest.fixture
def memjobstore(request):
    return MemoryJobStore()


@pytest.fixture
def sqlalchemyjobstore(request):
    def finish():
        store.close()
        os.remove('tempdb.sqlite')

    sqlalchemy = pytest.importorskip('apscheduler.jobstores.sqlalchemy')
    store = sqlalchemy.SQLAlchemyJobStore(url='sqlite:///tempdb.sqlite')
    request.addfinalizer(finish)
    return store


@pytest.fixture
def mongodbjobstore(request):
    def finish():
        connection = store.collection.database.connection
        connection.drop_database(store.collection.database.name)
        store.close()

    mongodb = pytest.importorskip('apscheduler.jobstores.mongodb')
    store = mongodb.MongoDBJobStore(database='apscheduler_unittest')
    request.addfinalizer(finish)
    return store


@pytest.fixture
def jobstore(request):
    return request.param(request)


def create_job(jobstore, func=dummy_job, trigger_date=datetime(2999, 1, 1), id=None):
    trigger_date.replace(tzinfo=local_tz)
    trigger = DateTrigger({}, trigger_date, local_tz)
    job = Job(trigger, func, [], {}, id, 1, False, None, None, 1)
    job.next_run_time = trigger.run_date
    jobstore.add_job(job)
    return job

persistent_jobstores = [sqlalchemyjobstore, mongodbjobstore]
persistent_jobstores_ids = ['sqlalchemy', 'mongodb']
all_jobstores = [memjobstore] + persistent_jobstores
all_jobstores_ids = ['memory'] + persistent_jobstores_ids


@pytest.mark.parametrize('jobstore', all_jobstores, indirect=True, ids=all_jobstores_ids)
def test_lookup_job(jobstore):
    initial_job = create_job(jobstore)
    job = jobstore.lookup_job(initial_job.id)
    assert job == initial_job


@pytest.mark.parametrize('jobstore', all_jobstores, indirect=True, ids=all_jobstores_ids)
def test_lookup_nonexistent_job(jobstore):
    pytest.raises(JobLookupError, jobstore.lookup_job, 'foo')


@pytest.mark.parametrize('jobstore', all_jobstores, indirect=True, ids=all_jobstores_ids)
def test_get_all_jobs(jobstore):
    job1 = create_job(jobstore, dummy_job, datetime(2016, 5, 3))
    job2 = create_job(jobstore, dummy_job2, datetime(2013, 8, 14))
    jobs = jobstore.get_all_jobs()
    assert jobs == [job2, job1]


@pytest.mark.parametrize('jobstore', all_jobstores, indirect=True, ids=all_jobstores_ids)
def test_get_pending_jobs(jobstore):
    job1 = create_job(jobstore, dummy_job, datetime(2016, 5, 3))
    job2 = create_job(jobstore, dummy_job2, datetime(2014, 2, 26))
    job3 = create_job(jobstore, dummy_job3, datetime(2013, 8, 14))
    jobs, next_run_time = jobstore.get_pending_jobs(datetime(2014, 2, 27, tzinfo=local_tz))
    assert jobs == [job3, job2]
    assert next_run_time == job1.trigger.run_date


@pytest.mark.parametrize('jobstore', all_jobstores, indirect=True, ids=all_jobstores_ids)
def test_get_pending_jobs_no_next(jobstore):
    job1 = create_job(jobstore, dummy_job, datetime(2016, 5, 3))
    job2 = create_job(jobstore, dummy_job2, datetime(2014, 2, 26))
    job3 = create_job(jobstore, dummy_job3, datetime(2013, 8, 14))
    jobs, next_run_time = jobstore.get_pending_jobs(datetime(2016, 5, 10, tzinfo=local_tz))
    assert jobs == [job3, job2, job1]
    assert next_run_time is None


@pytest.mark.parametrize('jobstore', all_jobstores, indirect=True, ids=all_jobstores_ids)
def test_add_job_conflicting_id(jobstore):
    create_job(jobstore, dummy_job, datetime(2016, 5, 3), id='blah')
    pytest.raises(ConflictingIdError, create_job, jobstore, dummy_job2, datetime(2014, 2, 26), id='blah')


@pytest.mark.parametrize('jobstore', all_jobstores, indirect=True, ids=all_jobstores_ids)
def test_update_job_id_and_others(jobstore):
    job1 = create_job(jobstore, dummy_job, datetime(2016, 5, 3))
    job2 = create_job(jobstore, dummy_job2, datetime(2014, 2, 26))
    jobstore.modify_job(job1.id, {'id': 'foo', 'max_instances': 6})

    jobs = jobstore.get_all_jobs()
    assert len(jobs) == 2
    assert jobs[0].id == job2.id
    assert jobs[1].id == 'foo'
    assert jobs[1].max_instances == 6


@pytest.mark.parametrize('jobstore', all_jobstores, indirect=True, ids=all_jobstores_ids)
def test_update_job_next_runtime(jobstore):
    job1 = create_job(jobstore, dummy_job, datetime(2016, 5, 3))
    job2 = create_job(jobstore, dummy_job2, datetime(2014, 2, 26))
    job3 = create_job(jobstore, dummy_job3, datetime(2013, 8, 14))
    jobstore.modify_job(job1.id, {'next_run_time': datetime(2014, 1, 3, tzinfo=local_tz)})

    jobs = jobstore.get_all_jobs()
    assert len(jobs) == 3
    assert jobs == [job3, job1, job2]


@pytest.mark.parametrize('jobstore', all_jobstores, indirect=True, ids=all_jobstores_ids)
def test_update_job_next_runtime_empty(jobstore):
    job1 = create_job(jobstore, dummy_job, datetime(2016, 5, 3))
    job2 = create_job(jobstore, dummy_job2, datetime(2014, 2, 26))
    jobstore.modify_job(job1.id, {'next_run_time': None})

    jobs = jobstore.get_all_jobs()
    assert len(jobs) == 2
    assert jobs == [job1, job2]


@pytest.mark.parametrize('jobstore', all_jobstores, indirect=True, ids=all_jobstores_ids)
def test_update_job_conflicting_id(jobstore):
    job1 = create_job(jobstore, dummy_job, datetime(2016, 5, 3))
    job2 = create_job(jobstore, dummy_job2, datetime(2014, 2, 26))
    pytest.raises(ConflictingIdError, jobstore.modify_job, job2.id, {'id': job1.id})


@pytest.mark.parametrize('jobstore', all_jobstores, indirect=True, ids=all_jobstores_ids)
def test_update_job_nonexistent_job(jobstore):
    pytest.raises(JobLookupError, jobstore.modify_job, 'foo', {'next_run_time': None})


@pytest.mark.parametrize('jobstore', persistent_jobstores, indirect=True, ids=persistent_jobstores_ids)
def test_one_job_fails_to_load(jobstore, monkeypatch):
    job1 = create_job(jobstore, dummy_job, datetime(2016, 5, 3))
    job2 = create_job(jobstore, dummy_job2, datetime(2014, 2, 26))
    job3 = create_job(jobstore, dummy_job3, datetime(2013, 8, 14))

    # Make the dummy_job2 function disappear
    monkeypatch.delitem(globals(), 'dummy_job2')

    jobs = jobstore.get_all_jobs()
    assert jobs == [job3, job1]


@pytest.mark.parametrize('jobstore', persistent_jobstores, indirect=True, ids=persistent_jobstores_ids)
def test_transient_job_error(jobstore):
    pytest.raises(TransientJobError, create_job, jobstore, lambda: None, datetime(2016, 5, 3))


@pytest.mark.parametrize('jobstore', all_jobstores, indirect=True, ids=all_jobstores_ids)
def test_remove_job(jobstore):
    job1 = create_job(jobstore, dummy_job, datetime(2016, 5, 3))
    job2 = create_job(jobstore, dummy_job2, datetime(2014, 2, 26))

    jobstore.remove_job(job1.id)
    jobs = jobstore.get_all_jobs()
    assert jobs == [job2]

    jobstore.remove_job(job2.id)
    jobs = jobstore.get_all_jobs()
    assert jobs == []


@pytest.mark.parametrize('jobstore', all_jobstores, indirect=True, ids=all_jobstores_ids)
def test_remove_nonexistent_job(jobstore):
    pytest.raises(JobLookupError, jobstore.remove_job, 'blah')


@pytest.mark.parametrize('jobstore', all_jobstores, indirect=True, ids=all_jobstores_ids)
def test_remove_all_jobs(jobstore):
    create_job(jobstore, dummy_job, datetime(2016, 5, 3))
    create_job(jobstore, dummy_job2, datetime(2014, 2, 26))

    jobstore.remove_all_jobs()
    jobs = jobstore.get_all_jobs()
    assert jobs == []


def test_repr_memjobstore(memjobstore):
    assert repr(memjobstore) == '<MemoryJobStore>'


def test_repr_sqlalchemyjobstore(sqlalchemyjobstore):
    assert repr(sqlalchemyjobstore) == '<SQLAlchemyJobStore (url=sqlite:///tempdb.sqlite)>'


def test_repr_mongodbjobstore(mongodbjobstore):
    assert repr(mongodbjobstore) == "<MongoDBJobStore (connection=Connection('localhost', 27017))>"


def test_memstore_close(memjobstore):
    create_job(memjobstore, dummy_job, datetime(2016, 5, 3))
    memjobstore.close()
    assert not memjobstore.get_all_jobs()


def test_sqlalchemy_engine_ref():
    global sqla_engine
    sqlalchemy = pytest.importorskip('apscheduler.jobstores.sqlalchemy')
    sqla_engine = sqlalchemy.create_engine('sqlite:///')
    try:
        sqlalchemy.SQLAlchemyJobStore(engine='%s:sqla_engine' % __name__)
    finally:
        sqla_engine.dispose()
        del sqla_engine


def test_sqlalchemy_missing_engine():
    sqlalchemy = pytest.importorskip('apscheduler.jobstores.sqlalchemy')
    exc = pytest.raises(ValueError, sqlalchemy.SQLAlchemyJobStore)
    assert 'Need either' in str(exc.value)


def test_mongodb_connection_ref():
    global mongodb_connection
    mongodb = pytest.importorskip('apscheduler.jobstores.mongodb')
    mongodb_connection = mongodb.Connection()
    try:
        mongodb.MongoDBJobStore(connection='%s:mongodb_connection' % __name__)
    finally:
        mongodb_connection.disconnect()
        del mongodb_connection


def test_mongodb_null_database():
    mongodb = pytest.importorskip('apscheduler.jobstores.mongodb')
    exc = pytest.raises(ValueError, mongodb.MongoDBJobStore, database='')
    assert '"database"' in str(exc.value)


def test_mongodb_null_collection():
    mongodb = pytest.importorskip('apscheduler.jobstores.mongodb')
    exc = pytest.raises(ValueError, mongodb.MongoDBJobStore, collection='')
    assert '"collection"' in str(exc.value)
