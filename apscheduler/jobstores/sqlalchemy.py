from __future__ import absolute_import

from apscheduler.jobstores.base import BaseJobStore, JobLookupError, ConflictingIdError
from apscheduler.util import maybe_ref, datetime_to_utc_timestamp, utc_timestamp_to_datetime
from apscheduler.job import Job

try:
    import cPickle as pickle
except ImportError:  # pragma: nocover
    import pickle

try:
    from sqlalchemy import (
        create_engine, Table, Column, MetaData, Unicode, Float, DateTime, 
        Integer, String, LargeBinary, select, ForeignKey)
    from sqlalchemy.exc import IntegrityError
    from sqlalchemy.sql.expression import null
except ImportError:  # pragma: nocover
    raise ImportError('SQLAlchemyJobStore requires SQLAlchemy installed')


class SQLAlchemyJobStore(BaseJobStore):
    """
    Stores jobs in a database table using SQLAlchemy.
    The table will be created if it doesn't exist in the database.

    Plugin alias: ``sqlalchemy``

    :param str url: connection string (see `SQLAlchemy documentation
        <http://docs.sqlalchemy.org/en/latest/core/engines.html?highlight=create_engine#database-urls>`_
        on this)
    :param engine: an SQLAlchemy Engine to use instead of creating a new one based on ``url``
    :param str tablename: name of the table to store jobs in
    :param metadata: a :class:`~sqlalchemy.MetaData` instance to use instead of creating a new one
    :param int pickle_protocol: pickle protocol level to use (for serialization), defaults to the
        highest available
    """

    def __init__(self, url=None, engine=None, tablename='apscheduler_jobs', metadata=None,
                 pickle_protocol=pickle.HIGHEST_PROTOCOL):
        super(SQLAlchemyJobStore, self).__init__()
        self.pickle_protocol = pickle_protocol
        metadata = maybe_ref(metadata) or MetaData()

        if engine:
            self.engine = maybe_ref(engine)
        elif url:
            self.engine = create_engine(url)
        else:
            raise ValueError('Need either "engine" or "url" defined')

        # 191 = max key length in MySQL for InnoDB/utf8mb4 tables,
        # 25 = precision that translates to an 8-byte float
        self.jobs_t = Table(
            tablename, metadata,
            Column('id', Unicode(191, _warn_on_bytestring=False), primary_key=True),
            Column('next_run_time', Float(25), index=True),
            Column('job_state', LargeBinary, nullable=False),

        )

        self.job_submissions_t = Table(
            "apscheduler_job_submissions", metadata,
            Column("id", primary_key=True),
            Column("state", Enum("submitted", "running", "success", "failure", "orphaned")),
            Column("func", String()),
            Column("submitted_at", DateTime()),
            Column("started_at", DateTime()),
            Column("completed_at", DateTime()),
            Column("apscheduler_job_id", Integer(), ForeignKey(tablename + ".id"))
        )
            

    def start(self, scheduler, alias):
        super(SQLAlchemyJobStore, self).start(scheduler, alias)
        self.jobs_t.create(self.engine, True)
        self.job_submissions_t.create(self.engine, True)

    def lookup_job(self, job_id):
        selectable = select([self.jobs_t.c.job_state]).where(self.jobs_t.c.id == job_id)
        job_state = self.engine.execute(selectable).scalar()
        return self._reconstitute_job(job_state) if job_state else None

    def get_due_jobs(self, now):
        timestamp = datetime_to_utc_timestamp(now)
        return self._get_jobs(self.jobs_t.c.next_run_time <= timestamp)

    def get_next_run_time(self):
        selectable = select([self.jobs_t.c.next_run_time]).\
            where(self.jobs_t.c.next_run_time != null()).\
            order_by(self.jobs_t.c.next_run_time).limit(1)
        next_run_time = self.engine.execute(selectable).scalar()
        return utc_timestamp_to_datetime(next_run_time)

    def add_job_submission(self, job):
        insert = self.job_submissions_t.insert().values(**{
            'state': 'submitted',
            'func': job.func,
            'submitted_at': datetime.now(),
            'apscheduler_job_id': job.id,
        })
        r = self.engine.execute(insert)
        job_submission_id = r.inserted_primary_key[0]
        return job_submission_id
    
    def update_job_submission(self, job_submission_id, **kwargs):
        update = self.job_submissions_t\
                .update(kwargs)\
                .where(job_submissions_t.c.id == job_submission_id)
        result = self.engine.execute(update)
        if result.rowcount == 0:
            raise JobInstanceLookupError(job_submission_id)
    
    def get_job_submissions_with_status(self, statuses=[]):
        selectable = select(map(lambda col: getattr(self.job_submissions_t.c, col),
            ["id", "state", "func", "submitted_at", "apscheduler_job_id"])).\
            order_by(self.job_submissions_t.c.submitted_at)
        if len(statuses) > 0:
            selectable = selectable.\
            where(self.job_submissions_t.c.status in statuses)
        job_instances = []
        for row in self.engine.execute(selectable):
            job_instances.append(dict(row))
        return jobs
    
    def get_job_submission(self, job_submission_id):
        selectable = self.job_submissions_t.select()\
                .where(self.submissions_t.c.id == job_submission_id)
        job_submission = self.engine.execute(selectable).scalar()
        return job_submission
    
    def get_all_jobs(self):
        jobs = self._get_jobs()
        self._fix_paused_jobs_sorting(jobs)
        return jobs

    def add_job(self, job):
        insert = self.jobs_t.insert().values(**{
            'id': job.id,
            'next_run_time': datetime_to_utc_timestamp(job.next_run_time),
            'job_state': pickle.dumps(job.__getstate__(), self.pickle_protocol)
        })
        try:
            self.engine.execute(insert)
        except IntegrityError:
            raise ConflictingIdError(job.id)

    def update_job(self, job):
        update = self.jobs_t.update().values(**{
            'next_run_time': datetime_to_utc_timestamp(job.next_run_time),
            'job_state': pickle.dumps(job.__getstate__(), self.pickle_protocol)
        }).where(self.jobs_t.c.id == job.id)
        result = self.engine.execute(update)
        if result.rowcount == 0:
            raise JobLookupError(id)

    def remove_job(self, job_id):
        delete = self.jobs_t.delete().where(self.jobs_t.c.id == job_id)
        result = self.engine.execute(delete)
        if result.rowcount == 0:
            raise JobLookupError(job_id)

    def remove_all_jobs(self):
        delete = self.jobs_t.delete()
        self.engine.execute(delete)

    def shutdown(self):
        self.engine.dispose()

    def _reconstitute_job(self, job_state):
        job_state = pickle.loads(job_state)
        job_state['jobstore'] = self
        job = Job.__new__(Job)
        job.__setstate__(job_state)
        job._scheduler = self._scheduler
        job._jobstore_alias = self._alias
        return job

    def _get_jobs(self, *conditions):
        jobs = []
        selectable = select([self.jobs_t.c.id, self.jobs_t.c.job_state]).\
            order_by(self.jobs_t.c.next_run_time)
        selectable = selectable.where(*conditions) if conditions else selectable
        failed_job_ids = set()
        for row in self.engine.execute(selectable):
            try:
                jobs.append(self._reconstitute_job(row.job_state))
            except:
                self._logger.exception('Unable to restore job "%s" -- removing it', row.id)
                failed_job_ids.add(row.id)

        # Remove all the jobs we failed to restore
        if failed_job_ids:
            delete = self.jobs_t.delete().where(self.jobs_t.c.id.in_(failed_job_ids))
            self.engine.execute(delete)

        return jobs

    def __repr__(self):
        return '<%s (url=%s)>' % (self.__class__.__name__, self.engine.url)
