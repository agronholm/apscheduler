from datetime import datetime
import os

import pytest

from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.jobstores.base import JobLookupError, ConflictingIdError, TransientJobError
from apscheduler.triggers.date import DateTrigger
from apscheduler.job import Job

run_time = datetime(2999, 1, 1)


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
        if os.path.exists('tempdb.sqlite'):
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


@pytest.fixture(params=[memjobstore, sqlalchemyjobstore, mongodbjobstore], ids=['memory', 'sqlalchemy', 'mongodb'])
def jobstore(request):
    return request.param(request)


@pytest.fixture(params=[sqlalchemyjobstore, mongodbjobstore], ids=['sqlalchemy', 'mongodb'])
def persistent_jobstore(request):
    return request.param(request)


@pytest.fixture
def create_job(timezone, job_defaults):
    def create(jobstore, func=dummy_job, trigger_date=run_time, id=None):
        trigger_date = trigger_date.replace(tzinfo=timezone)
        trigger = DateTrigger(timezone, trigger_date)
        job_kwargs = job_defaults.copy()
        job_kwargs['func'] = func
        job_kwargs['trigger'] = trigger
        job_kwargs['id'] = id
        job = Job(**job_kwargs)
        job.next_run_time = job.trigger.get_next_fire_time(trigger_date)
        jobstore.add_job(job)
        return job

    return create


def test_lookup_job(jobstore, create_job):
    initial_job = create_job(jobstore)
    job = jobstore.lookup_job(initial_job.id)
    assert job == initial_job


def test_lookup_nonexistent_job(jobstore):
    pytest.raises(JobLookupError, jobstore.lookup_job, 'foo')


def test_get_all_jobs(jobstore, create_job):
    job1 = create_job(jobstore, dummy_job, datetime(2016, 5, 3))
    job2 = create_job(jobstore, dummy_job2, datetime(2013, 8, 14))
    jobs = jobstore.get_all_jobs()
    assert jobs == [job2, job1]


def test_get_pending_jobs(jobstore, create_job, timezone):
    job1 = create_job(jobstore, dummy_job, datetime(2016, 5, 3))
    job2 = create_job(jobstore, dummy_job2, datetime(2014, 2, 26))
    job3 = create_job(jobstore, dummy_job3, datetime(2013, 8, 14))
    jobs, next_run_time = jobstore.get_pending_jobs(datetime(2014, 2, 27, tzinfo=timezone))
    assert jobs == [job3, job2]
    assert next_run_time == job1.trigger.run_date


def test_get_pending_jobs_no_next(jobstore, create_job, timezone):
    job1 = create_job(jobstore, dummy_job, datetime(2016, 5, 3))
    job2 = create_job(jobstore, dummy_job2, datetime(2014, 2, 26))
    job3 = create_job(jobstore, dummy_job3, datetime(2013, 8, 14))
    jobs, next_run_time = jobstore.get_pending_jobs(datetime(2016, 5, 10, tzinfo=timezone))
    assert jobs == [job3, job2, job1]
    assert next_run_time is None


def test_add_job_conflicting_id(jobstore, create_job):
    create_job(jobstore, dummy_job, datetime(2016, 5, 3), id='blah')
    pytest.raises(ConflictingIdError, create_job, jobstore, dummy_job2, datetime(2014, 2, 26), id='blah')


def test_update_job_id_and_others(jobstore, create_job):
    job1 = create_job(jobstore, dummy_job, datetime(2016, 5, 3))
    job2 = create_job(jobstore, dummy_job2, datetime(2014, 2, 26))
    jobstore.modify_job(job1.id, {'id': 'foo', 'max_instances': 6})

    jobs = jobstore.get_all_jobs()
    assert len(jobs) == 2
    assert jobs[0].id == job2.id
    assert jobs[1].id == 'foo'
    assert jobs[1].max_instances == 6


def test_update_job_next_runtime(jobstore, create_job, timezone):
    job1 = create_job(jobstore, dummy_job, datetime(2016, 5, 3))
    job2 = create_job(jobstore, dummy_job2, datetime(2014, 2, 26))
    job3 = create_job(jobstore, dummy_job3, datetime(2013, 8, 14))
    jobstore.modify_job(job1.id, {'next_run_time': datetime(2014, 1, 3, tzinfo=timezone)})

    jobs = jobstore.get_all_jobs()
    assert len(jobs) == 3
    assert jobs == [job3, job1, job2]


def test_update_job_next_runtime_empty(jobstore, create_job):
    job1 = create_job(jobstore, dummy_job, datetime(2016, 5, 3))
    job2 = create_job(jobstore, dummy_job2, datetime(2014, 2, 26))
    jobstore.modify_job(job1.id, {'next_run_time': None})

    jobs = jobstore.get_all_jobs()
    assert len(jobs) == 2
    assert jobs == [job1, job2]


def test_update_job_conflicting_id(jobstore, create_job):
    job1 = create_job(jobstore, dummy_job, datetime(2016, 5, 3))
    job2 = create_job(jobstore, dummy_job2, datetime(2014, 2, 26))
    pytest.raises(ConflictingIdError, jobstore.modify_job, job2.id, {'id': job1.id})


def test_update_job_nonexistent_job(jobstore):
    pytest.raises(JobLookupError, jobstore.modify_job, 'foo', {'next_run_time': None})


def test_one_job_fails_to_load(persistent_jobstore, create_job, monkeypatch):
    job1 = create_job(persistent_jobstore, dummy_job, datetime(2016, 5, 3))
    job2 = create_job(persistent_jobstore, dummy_job2, datetime(2014, 2, 26))
    job3 = create_job(persistent_jobstore, dummy_job3, datetime(2013, 8, 14))

    # Make the dummy_job2 function disappear
    monkeypatch.delitem(globals(), 'dummy_job2')

    jobs = persistent_jobstore.get_all_jobs()
    assert jobs == [job3, job1]


def test_transient_job_error(persistent_jobstore, create_job):
    pytest.raises(TransientJobError, create_job, persistent_jobstore, lambda: None, datetime(2016, 5, 3))


def test_remove_job(jobstore, create_job):
    job1 = create_job(jobstore, dummy_job, datetime(2016, 5, 3))
    job2 = create_job(jobstore, dummy_job2, datetime(2014, 2, 26))

    jobstore.remove_job(job1.id)
    jobs = jobstore.get_all_jobs()
    assert jobs == [job2]

    jobstore.remove_job(job2.id)
    jobs = jobstore.get_all_jobs()
    assert jobs == []


def test_remove_nonexistent_job(jobstore):
    pytest.raises(JobLookupError, jobstore.remove_job, 'blah')


def test_remove_all_jobs(jobstore, create_job):
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


def test_memstore_close(memjobstore, create_job):
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
