from datetime import datetime
import os

import pytest

from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.jobstores.base import JobLookupError, ConflictingIdError


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
        store.client.drop_database(store.collection.database.name)
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
def create_add_job(timezone, create_job):
    def create(jobstore, func=dummy_job, run_date=datetime(2999, 1, 1), id=None, paused=False, **kwargs):
        run_date = timezone.localize(run_date)
        job = create_job(func=func, trigger='date', trigger_args={'run_date': run_date}, id=id, **kwargs)
        job.next_run_time = None if paused else job.trigger.get_next_fire_time(None, run_date)
        if jobstore:
            jobstore.add_job(job)
        return job

    return create


def test_lookup_job(jobstore, create_add_job):
    initial_job = create_add_job(jobstore)
    job = jobstore.lookup_job(initial_job.id)
    assert job == initial_job


def test_lookup_nonexistent_job(jobstore):
    assert jobstore.lookup_job('foo') is None


def test_get_all_jobs(jobstore, create_add_job):
    job1 = create_add_job(jobstore, dummy_job, datetime(2016, 5, 3))
    job2 = create_add_job(jobstore, dummy_job2, datetime(2013, 8, 14))
    job3 = create_add_job(jobstore, dummy_job2, datetime(2013, 7, 11), paused=True)
    jobs = jobstore.get_all_jobs()
    assert jobs == [job2, job1, job3]


def test_get_pending_jobs(jobstore, create_add_job, timezone):
    create_add_job(jobstore, dummy_job, datetime(2016, 5, 3))
    job2 = create_add_job(jobstore, dummy_job2, datetime(2014, 2, 26))
    job3 = create_add_job(jobstore, dummy_job3, datetime(2013, 8, 14))
    create_add_job(jobstore, dummy_job3, datetime(2013, 7, 11), paused=True)
    jobs = jobstore.get_due_jobs(timezone.localize(datetime(2014, 2, 27)))
    assert jobs == [job3, job2]

    jobs = jobstore.get_due_jobs(timezone.localize(datetime(2013, 8, 13)))
    assert jobs == []


def test_get_pending_jobs_subsecond_difference(jobstore, create_add_job, timezone):
    job1 = create_add_job(jobstore, dummy_job, datetime(2014, 7, 7, 0, 0, 0, 401))
    job2 = create_add_job(jobstore, dummy_job2, datetime(2014, 7, 7, 0, 0, 0, 402))
    job3 = create_add_job(jobstore, dummy_job3, datetime(2014, 7, 7, 0, 0, 0, 400))
    jobs = jobstore.get_due_jobs(timezone.localize(datetime(2014, 7, 7, 1)))
    assert jobs == [job3, job1, job2]


def test_get_next_run_time(jobstore, create_add_job, timezone):
    create_add_job(jobstore, dummy_job, datetime(2016, 5, 3))
    create_add_job(jobstore, dummy_job2, datetime(2014, 2, 26))
    create_add_job(jobstore, dummy_job3, datetime(2013, 8, 14))
    create_add_job(jobstore, dummy_job3, datetime(2013, 7, 11), paused=True)
    assert jobstore.get_next_run_time() == timezone.localize(datetime(2013, 8, 14))


def test_add_job_conflicting_id(jobstore, create_add_job):
    create_add_job(jobstore, dummy_job, datetime(2016, 5, 3), id='blah')
    pytest.raises(ConflictingIdError, create_add_job, jobstore, dummy_job2, datetime(2014, 2, 26), id='blah')


def test_update_job(jobstore, create_add_job, timezone):
    job1 = create_add_job(jobstore, dummy_job, datetime(2016, 5, 3))
    job2 = create_add_job(jobstore, dummy_job2, datetime(2014, 2, 26))
    replacement = create_add_job(None, dummy_job, datetime(2016, 5, 4), id=job1.id, max_instances=6)
    assert replacement.max_instances == 6
    jobstore.update_job(replacement)

    jobs = jobstore.get_all_jobs()
    assert len(jobs) == 2
    assert jobs[0].id == job2.id
    assert jobs[1].id == job1.id
    assert jobs[1].next_run_time == timezone.localize(datetime(2016, 5, 4))
    assert jobs[1].max_instances == 6


@pytest.mark.parametrize('next_run_time', [datetime(2013, 8, 13), None], ids=['earlier', 'null'])
def test_update_job_next_runtime(jobstore, create_add_job, next_run_time, timezone):
    job1 = create_add_job(jobstore, dummy_job, datetime(2016, 5, 3))
    create_add_job(jobstore, dummy_job2, datetime(2014, 2, 26))
    job3 = create_add_job(jobstore, dummy_job3, datetime(2013, 8, 14))
    job1.next_run_time = timezone.localize(next_run_time) if next_run_time else None
    jobstore.update_job(job1)

    if next_run_time:
        assert jobstore.get_next_run_time() == job1.next_run_time
    else:
        assert jobstore.get_next_run_time() == job3.next_run_time


@pytest.mark.parametrize('next_run_time', [datetime(2013, 8, 13), None], ids=['earlier', 'null'])
@pytest.mark.parametrize('index', [0, 1, 2], ids=['first', 'middle', 'last'])
def test_update_job_clear_next_runtime(jobstore, create_add_job, next_run_time, timezone, index):
    """
    Tests that update_job() maintains the proper ordering of the jobs, even when their next run times are initially the
    same.
    """

    jobs = [create_add_job(jobstore, dummy_job, datetime(2014, 2, 26), 'job%d' % i) for i in range(3)]
    jobs[index].next_run_time = timezone.localize(next_run_time) if next_run_time else None
    jobstore.update_job(jobs[index])
    due_date = timezone.localize(datetime(2014, 2, 27))
    due_jobs = jobstore.get_due_jobs(due_date)

    assert len(due_jobs) == (3 if next_run_time else 2)
    due_job_ids = [job.id for job in due_jobs]
    if next_run_time:
        if index == 0:
            assert due_job_ids == ['job0', 'job1', 'job2']
        elif index == 1:
            assert due_job_ids == ['job1', 'job0', 'job2']
        else:
            assert due_job_ids == ['job2', 'job0', 'job1']
    else:
        if index == 0:
            assert due_job_ids == ['job1', 'job2']
        elif index == 1:
            assert due_job_ids == ['job0', 'job2']
        else:
            assert due_job_ids == ['job0', 'job1']


def test_update_job_nonexistent_job(jobstore, create_add_job):
    job = create_add_job(None, dummy_job, datetime(2016, 5, 3))
    pytest.raises(JobLookupError, jobstore.update_job, job)


def test_one_job_fails_to_load(persistent_jobstore, create_add_job, monkeypatch, timezone):
    job1 = create_add_job(persistent_jobstore, dummy_job, datetime(2016, 5, 3))
    job2 = create_add_job(persistent_jobstore, dummy_job2, datetime(2014, 2, 26))
    create_add_job(persistent_jobstore, dummy_job3, datetime(2013, 8, 14))

    # Make the dummy_job2 function disappear
    monkeypatch.delitem(globals(), 'dummy_job3')

    jobs = persistent_jobstore.get_all_jobs()
    assert jobs == [job2, job1]

    assert persistent_jobstore.get_next_run_time() == timezone.localize(datetime(2014, 2, 26))


def test_remove_job(jobstore, create_add_job):
    job1 = create_add_job(jobstore, dummy_job, datetime(2016, 5, 3))
    job2 = create_add_job(jobstore, dummy_job2, datetime(2014, 2, 26))

    jobstore.remove_job(job1.id)
    jobs = jobstore.get_all_jobs()
    assert jobs == [job2]

    jobstore.remove_job(job2.id)
    jobs = jobstore.get_all_jobs()
    assert jobs == []


def test_remove_nonexistent_job(jobstore):
    pytest.raises(JobLookupError, jobstore.remove_job, 'blah')


def test_remove_all_jobs(jobstore, create_add_job):
    create_add_job(jobstore, dummy_job, datetime(2016, 5, 3))
    create_add_job(jobstore, dummy_job2, datetime(2014, 2, 26))

    jobstore.remove_all_jobs()
    jobs = jobstore.get_all_jobs()
    assert jobs == []


def test_repr_memjobstore(memjobstore):
    assert repr(memjobstore) == '<MemoryJobStore>'


def test_repr_sqlalchemyjobstore(sqlalchemyjobstore):
    assert repr(sqlalchemyjobstore) == '<SQLAlchemyJobStore (url=sqlite:///apscheduler_unittest.sqlite)>'


def test_repr_mongodbjobstore(mongodbjobstore):
    assert repr(mongodbjobstore) == "<MongoDBJobStore (client=MongoClient('localhost', 27017))>"


def test_repr_redisjobstore(redisjobstore):
    assert repr(redisjobstore) == '<RedisJobStore>'


def test_memstore_close(memjobstore, create_add_job):
    create_add_job(memjobstore, dummy_job, datetime(2016, 5, 3))
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


def test_mongodb_client_ref():
    global mongodb_client
    mongodb = pytest.importorskip('apscheduler.jobstores.mongodb')
    mongodb_client = mongodb.MongoClient()
    try:
        mongodb.MongoDBJobStore(client='%s:mongodb_client' % __name__)
    finally:
        mongodb_client.close()
        del mongodb_client


def test_mongodb_null_database():
    mongodb = pytest.importorskip('apscheduler.jobstores.mongodb')
    exc = pytest.raises(ValueError, mongodb.MongoDBJobStore, database='')
    assert '"database"' in str(exc.value)


def test_mongodb_null_collection():
    mongodb = pytest.importorskip('apscheduler.jobstores.mongodb')
    exc = pytest.raises(ValueError, mongodb.MongoDBJobStore, collection='')
    assert '"collection"' in str(exc.value)
