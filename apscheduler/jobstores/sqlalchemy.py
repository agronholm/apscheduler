"""
Stores jobs in a database table using SQLAlchemy.
"""
from __future__ import absolute_import
import pickle
import logging

import six

from apscheduler.jobstores.base import BaseJobStore, JobLookupError, ConflictingIdError, TransientJobError
from apscheduler.util import maybe_ref, datetime_to_utc_timestamp, utc_timestamp_to_datetime
from apscheduler.job import Job

try:
    from sqlalchemy import (create_engine, Table, PickleType, Column, MetaData, Unicode, BigInteger, LargeBinary,
                            select, __version__ as sqlalchemy_version)
    from sqlalchemy.exc import IntegrityError
except ImportError:  # pragma: nocover
    raise ImportError('SQLAlchemyJobStore requires SQLAlchemy installed')

logger = logging.getLogger(__name__)


class SQLAlchemyJobStore(BaseJobStore):
    def __init__(self, url=None, engine=None, tablename='apscheduler_jobs', metadata=None,
                 pickle_protocol=pickle.HIGHEST_PROTOCOL):
        self.pickle_protocol = pickle_protocol
        metadata = maybe_ref(metadata) or MetaData()

        if engine:
            self.engine = maybe_ref(engine)
        elif url:
            self.engine = create_engine(url, echo=True)
        else:
            raise ValueError('Need either "engine" or "url" defined')

        if sqlalchemy_version < '0.7':  # pragma: nocover
            pickle_coltype = PickleType(pickle_protocol, mutable=False)
        else:
            pickle_coltype = PickleType(pickle_protocol)
        self.jobs_t = Table(
            tablename, metadata,
            Column('id', Unicode(1024), primary_key=True),
            Column('next_run_time', BigInteger, index=True),
            Column('job_data', pickle_coltype, nullable=False)
        )

        self.jobs_t.create(self.engine, True)

    def lookup_job(self, id):
        selectable = select([self.jobs_t]).where(self.jobs_t.c.id == id)
        row = self.engine.execute(selectable).fetchone()
        if row is None:
            raise JobLookupError(id)
        return self._reconstitute_job(row)

    def get_pending_jobs(self, now):
        timestamp = datetime_to_utc_timestamp(now)
        selectable = select([self.jobs_t]).where(self.jobs_t.c.next_run_time <= timestamp).\
            order_by(self.jobs_t.c.next_run_time)
        jobs = self._get_jobs(selectable)

        selectable = select([self.jobs_t.c.next_run_time]).where(self.jobs_t.c.next_run_time > timestamp).\
            order_by(self.jobs_t.c.next_run_time).limit(1)
        next_run_time = self.engine.execute(selectable).scalar()
        return jobs, utc_timestamp_to_datetime(next_run_time)

    def get_all_jobs(self):
        selectable = select([self.jobs_t]).order_by(self.jobs_t.c.next_run_time)
        return self._get_jobs(selectable)

    def add_job(self, job):
        job_dict = job.__getstate__()
        if not job_dict.get('func'):
            raise TransientJobError(job.id)

        row_dict = {
            'id': job_dict.pop('id'),
            'next_run_time': datetime_to_utc_timestamp(job_dict['next_run_time']),
            'job_data': job_dict
        }

        insert = self.jobs_t.insert().values(**row_dict)
        try:
            self.engine.execute(insert)
        except IntegrityError:
            raise ConflictingIdError(job.id)

    def modify_job(self, id, changes):
        selectable = select([self.jobs_t]).where(self.jobs_t.c.id == id)
        row = self.engine.execute(selectable).fetchone()
        if row is None:
            raise JobLookupError(id)
        job_dict = dict(row.items())['job_data']

        row_changes = {}
        if 'id' in changes:
            row_changes['id'] = changes.pop('id')
        if 'next_run_time' in changes:
            row_changes['next_run_time'] = datetime_to_utc_timestamp(changes['next_run_time'])
        if changes:
            job_dict.update(changes)
            row_changes['job_data'] = job_dict

        update = self.jobs_t.update().where(self.jobs_t.c.id == id).values(**row_changes)
        try:
            self.engine.execute(update)
        except IntegrityError:
            raise ConflictingIdError(row_changes['id'])

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

    def _reconstitute_job(self, row):
        job = Job.__new__(Job)
        job_dict = dict(id=row.id, **row.job_data)
        job.__setstate__(job_dict)
        return job

    def _get_jobs(self, selectable):
        jobs = []
        for row in self.engine.execute(selectable):
            try:
                job = self._reconstitute_job(row)
                jobs.append(job)
            except Exception:
                logger.exception(six.u('Unable to restore job (id=%s)'), row.id)
        return jobs

    def __repr__(self):
        return '<%s (url=%s)>' % (self.__class__.__name__, self.engine.url)
