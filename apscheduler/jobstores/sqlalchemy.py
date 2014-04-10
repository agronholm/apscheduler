"""
Stores jobs in a database table using SQLAlchemy.
"""
from __future__ import absolute_import
import logging

import six

from apscheduler.jobstores.base import BaseJobStore, JobLookupError, ConflictingIdError
from apscheduler.util import maybe_ref, datetime_to_utc_timestamp, utc_timestamp_to_datetime
from apscheduler.job import Job

try:
    import cPickle as pickle
except ImportError:  # pragma: nocover
    import pickle

try:
    from sqlalchemy import create_engine, Table, Column, MetaData, Unicode, BigInteger, LargeBinary, select
    from sqlalchemy.exc import IntegrityError
except ImportError:  # pragma: nocover
    raise ImportError('SQLAlchemyJobStore requires SQLAlchemy installed')

logger = logging.getLogger(__name__)


class SQLAlchemyJobStore(BaseJobStore):
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

        self.jobs_t = Table(
            tablename, metadata,
            Column('id', Unicode(1024, _warn_on_bytestring=False), primary_key=True),
            Column('next_run_time', BigInteger, index=True),
            Column('job_state', LargeBinary, nullable=False)
        )

        self.jobs_t.create(self.engine, True)

    def lookup_job(self, id):
        selectable = select([self.jobs_t.c.job_state]).where(self.jobs_t.c.id == id)
        job_state = self.engine.execute(selectable).scalar()
        if job_state is None:
            raise JobLookupError(id)
        return self._reconstitute_job(job_state)

    def get_pending_jobs(self, now):
        timestamp = datetime_to_utc_timestamp(now)
        return self._get_jobs(self.jobs_t.c.next_run_time <= timestamp)

    def get_next_run_time(self):
        selectable = select([self.jobs_t.c.next_run_time]).where(self.jobs_t.c.next_run_time != None).\
            order_by(self.jobs_t.c.next_run_time).limit(1)
        next_run_time = self.engine.execute(selectable).scalar()
        return utc_timestamp_to_datetime(next_run_time)

    def get_all_jobs(self):
        return self._get_jobs()

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

    def remove_job(self, id):
        delete = self.jobs_t.delete().where(self.jobs_t.c.id == id)
        result = self.engine.execute(delete)
        if result.rowcount == 0:
            raise JobLookupError(id)

    def remove_all_jobs(self):
        delete = self.jobs_t.delete()
        self.engine.execute(delete)

    def close(self):
        self.engine.dispose()

    @staticmethod
    def _reconstitute_job(job_state):
        job_state = pickle.loads(job_state)
        job = Job.__new__(Job)
        job.__setstate__(job_state)
        return job

    def _get_jobs(self, *conditions):
        jobs = []
        selectable = select([self.jobs_t.c.id, self.jobs_t.c.job_state]).order_by(self.jobs_t.c.next_run_time)
        selectable = selectable.where(*conditions) if conditions else selectable
        for row in self.engine.execute(selectable):
            try:
                jobs.append(self._reconstitute_job(row.job_state))
            except Exception:
                logger.exception(six.u('Unable to restore job (id=%s)'), row.id)

        return jobs

    def __repr__(self):
        return '<%s (url=%s)>' % (self.__class__.__name__, self.engine.url)
