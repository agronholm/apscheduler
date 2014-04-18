from datetime import datetime
import os

import pytest

from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.jobstores.base import JobLookupError, ConflictingIdError
from apscheduler.triggers.date import DateTrigger
from apscheduler.job import Job


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
        store.shutdown()
        if os.path.exists('apscheduler_unittest.sqlite'):
            os.remove('apscheduler_unittest.sqlite')

    sqlalchemy = pytest.importorskip('apscheduler.jobstores.sqlalchemy')
    store = sqlalchemy.SQLAlchemyJobStore(url='sqlite:///apscheduler_unittest.sqlite')
    request.addfinalizer(finish)
    return store


@pytest.fixture
def mongodbjobstore(request):
    def finish():
        connection = store.collection.database.connection
        connection.drop_database(store.collection.database.name)
        store.shutdown()

    mongodb = pytest.importorskip('apscheduler.jobstores.mongodb')
    store = mongodb.MongoDBJobStore(database='apscheduler_unittest')
    request.addfinalizer(finish)
    return store


@pytest.fixture
def redisjobstore(request):
    def finish():
        store.remove_all_jobs()
        store.shutdown()

    redis = pytest.importorskip('apscheduler.jobstores.redis')
    store = redis.RedisJobStore()
    request.addfinalizer(finish)
    return store


@pytest.fixture(params=[memjobstore, sqlalchemyjobstore, mongodbjobstore, redisjobstore],
                ids=['memory', 'sqlalchemy', 'mongodb', 'redis'])
def jobstore(request):
    return request.param(request)


@pytest.fixture(params=[sqlalchemyjobstore, mongodbjobstore, redisjobstore], ids=['sqlalchemy', 'mongodb', 'redis'])
def persistent_jobstore(request):
    return request.param(request)


@pytest.fixture
def create_job(timezone, job_defaults):
    def create(jobstore, func=dummy_job, trigger_date=datetime(2999, 1, 1), id=None, **kwargs):
        trigger_date = trigger_date.replace(tzinfo=timezone)
        trigger = DateTrigger(trigger_date, timezone)
        job_kwargs = job_defaults.copy()
        job_kwargs['func'] = func
        job_kwargs['trigger'] = trigger
        job_kwargs['id'] = id
        job_kwargs.update(kwargs)
        job = Job(**job_kwargs)
        job.next_run_time = job.trigger.get_next_fire_time(trigger_date)
        if jobstore:
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
    create_job(jobstore, dummy_job, datetime(2016, 5, 3))
    job2 = create_job(jobstore, dummy_job2, datetime(2014, 2, 26))
    job3 = create_job(jobstore, dummy_job3, datetime(2013, 8, 14))
    jobs = jobstore.get_pending_jobs(datetime(2014, 2, 27, tzinfo=timezone))
    assert jobs == [job3, job2]

    jobs = jobstore.get_pending_jobs(datetime(2013, 8, 13, tzinfo=timezone))
    assert jobs == []


def test_get_next_run_time(jobstore, create_job, timezone):
    create_job(jobstore, dummy_job, datetime(2016, 5, 3))
    create_job(jobstore, dummy_job2, datetime(2014, 2, 26))
    create_job(jobstore, dummy_job3, datetime(2013, 8, 14))
    assert jobstore.get_next_run_time() == datetime(2013, 8, 14, tzinfo=timezone)


def test_add_job_conflicting_id(jobstore, create_job):
    create_job(jobstore, dummy_job, datetime(2016, 5, 3), id='blah')
    pytest.raises(ConflictingIdError, create_job, jobstore, dummy_job2, datetime(2014, 2, 26), id='blah')


def test_update_job(jobstore, create_job, timezone):
    job1 = create_job(jobstore, dummy_job, datetime(2016, 5, 3))
    job2 = create_job(jobstore, dummy_job2, datetime(2014, 2, 26))
    replacement = create_job(None, dummy_job, datetime(2016, 5, 4), id=job1.id, max_instances=6)
    jobstore.update_job(replacement)

    jobs = jobstore.get_all_jobs()
    assert len(jobs) == 2
    assert jobs[0].id == job2.id
    assert jobs[1].id == job1.id
    assert jobs[1].next_run_time == datetime(2016, 5, 4, tzinfo=timezone)
    assert jobs[1].max_instances == 6


@pytest.mark.parametrize('next_run_time', [datetime(2013, 8, 13), None], ids=['earlier', 'null'])
def test_update_job_next_runtime(jobstore, create_job, next_run_time, timezone):
    job1 = create_job(jobstore, dummy_job, datetime(2016, 5, 3))
    job2 = create_job(jobstore, dummy_job2, datetime(2014, 2, 26))
    job3 = create_job(jobstore, dummy_job3, datetime(2013, 8, 14))
    replacement = create_job(None, dummy_job, datetime.now(), id=job1.id)
    replacement.next_run_time = next_run_time.replace(tzinfo=timezone) if next_run_time else None
    jobstore.update_job(replacement)

    if next_run_time:
        assert jobstore.get_next_run_time() == replacement.next_run_time
    else:
        assert jobstore.get_next_run_time() == job3.next_run_time


def test_update_job_nonexistent_job(jobstore, create_job):
    job = create_job(None, dummy_job, datetime(2016, 5, 3))
    pytest.raises(JobLookupError, jobstore.update_job, job)


def test_one_job_fails_to_load(persistent_jobstore, create_job, monkeypatch):
    job1 = create_job(persistent_jobstore, dummy_job, datetime(2016, 5, 3))
    job2 = create_job(persistent_jobstore, dummy_job2, datetime(2014, 2, 26))
    job3 = create_job(persistent_jobstore, dummy_job3, datetime(2013, 8, 14))

    # Make the dummy_job2 function disappear
    monkeypatch.delitem(globals(), 'dummy_job2')

    jobs = persistent_jobstore.get_all_jobs()
    assert jobs == [job3, job1]


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
    assert repr(sqlalchemyjobstore) == '<SQLAlchemyJobStore (url=sqlite:///apscheduler_unittest.sqlite)>'


def test_repr_mongodbjobstore(mongodbjobstore):
    assert repr(mongodbjobstore) == "<MongoDBJobStore (connection=Connection('localhost', 27017))>"


def test_repr_memjobstore(redisjobstore):
    assert repr(redisjobstore) == '<RedisJobStore>'


def test_memstore_close(memjobstore, create_job):
    create_job(memjobstore, dummy_job, datetime(2016, 5, 3))
    memjobstore.shutdown()
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
