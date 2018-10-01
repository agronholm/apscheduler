from __future__ import absolute_import

from enum import Enum

from apscheduler.jobstores.base import BaseJobStore, JobLookupError, ConflictingIdError
from apscheduler.util import maybe_ref, datetime_to_utc_timestamp, utc_timestamp_to_datetime
from apscheduler.job import Job

try:
    import cPickle as pickle
except ImportError:  # pragma: nocover
    import pickle

try:
    from peewee import (SqliteDatabase, MySQLDatabase, PostgresqlDatabase, Model, TextField,
                        BlobField, FloatField, IntegrityError)
except ImportError:  # pragma: nocover
    raise ImportError('PeeweeJobStore requires peewee installed')


class Databases(Enum):
    SQLITE = 'sqlite'
    MYSQL = 'mysql'
    POSTGRES = 'postgres'


class PeeweeJobStore(BaseJobStore):
    """

    Stores jobs in a database table using Peewee.
    The table will be created if it doesn't exist in the database.

    Plugin alias: ``peewee``

    :param database: path or connections string depending on the database type.
        see http://docs.peewee-orm.com/en/latest/peewee/database.html
    :param database_type: Type of database, value must exist in :class`:Database`.
    :param database_ref: a Peewee :class:`~peewee.Database` to use instead of creating a
        new one based on ``database``
    :param str tablename: name of the table to store jobs in
    :param int pickle_protocol: pickle protocol level to use (for serialization), defaults to the
        highest available
    :param str tableschema: name of the (existing) schema in the target database where the table
        should be
    :param dict database_options: keyword arguments specifc to the database type
        (ignored if ``database_ref`` is given)
    """

    def __init__(self, database=None, database_type=None, database_ref=None,
                 tablename='apscheduler_jobs', pickle_protocol=pickle.HIGHEST_PROTOCOL,
                 tableschema=None, database_options=None):
        super(PeeweeJobStore, self).__init__()
        self.pickle_protocol = pickle_protocol

        if database_ref:
            self.database = maybe_ref(database_ref)
        elif database and database_type:
            self.database = self._init_database(database, database_type, database_options)
        else:
            raise ValueError('Need either "database" and "database_type" or "database_ref"'
                             ' defined')

        # incase there is a connection issue it will be found here
        # instead of downstream
        self.database.connect()

        class PeeweeJob(Model):
            class Meta:
                database = self.database
                table_name = tablename
                schema = tableschema

            id = TextField(primary_key=True)
            next_run_time = FloatField(null=True)
            job_state = BlobField()

        self.job_model = PeeweeJob


    def _init_database(self, database, database_type_str, database_options):
        # Will raise ValueError if database type is not supported
        database_type = Databases(database_type_str)

        if database_type == Databases.SQLITE:
            return SqliteDatabase(database, **(database_options or {}))
        elif database_type == Databases.MYSQL:
            return MySQLDatabase(database, **(database_options or {}))
        elif database_type == Databases.POSTGRES:
            return PostgresqlDatabase(database, **(database_options or {}))

    def start(self, scheduler, alias):
        super(PeeweeJobStore, self).start(scheduler, alias)
        self.database.create_tables([self.job_model], safe=True)

    def lookup_job(self, job_id):
        selectable = self.job_model.select(self.job_model.job_state).\
                where(self.job_model.id == job_id)
        job = selectable.first()
        return self._reconstitute_job(job.job_state) if job else None

    def get_due_jobs(self, now):
        timestamp = datetime_to_utc_timestamp(now)
        return self._get_jobs(self.job_model.next_run_time <= timestamp)

    def get_next_run_time(self):
        selectable = self.job_model.select(self.job_model.next_run_time).\
            where(~(self.job_model.next_run_time.is_null())).\
            order_by(self.job_model.next_run_time.asc()).limit(1)
        job = selectable.first()
        return utc_timestamp_to_datetime(job.next_run_time) if job else None

    def get_all_jobs(self):
        jobs = self._get_jobs()
        self._fix_paused_jobs_sorting(jobs)
        return jobs

    def add_job(self, job):
        q = self.job_model.insert(**{
            'id': job.id,
            'next_run_time': datetime_to_utc_timestamp(job.next_run_time),
            'job_state': pickle.dumps(job.__getstate__(), self.pickle_protocol)
        })
        try:
            q.execute()
        except IntegrityError:
            raise ConflictingIdError(job.id)

    def update_job(self, job):
        q = self.job_model.update(**{
            'next_run_time': datetime_to_utc_timestamp(job.next_run_time),
            'job_state': pickle.dumps(job.__getstate__(), self.pickle_protocol)
        }).where(self.job_model.id == job.id)
        rowcount = q.execute()
        if rowcount == 0:
            raise JobLookupError(job.id)

    def remove_job(self, job_id):
        dq = self.job_model.delete().where(self.job_model.id == job_id)
        rowcount = dq.execute()
        if rowcount == 0:
            raise JobLookupError(job_id)

    def remove_all_jobs(self):
        dq = self.job_model.delete()
        dq.execute()

    def shutdown(self):
        self.database.close()

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
        selectable = self.job_model.select(self.job_model.id, self.job_model.job_state).\
            order_by(self.job_model.next_run_time.asc())
        selectable = selectable.where(*conditions) if conditions else selectable
        failed_job_ids = set()
        for row in selectable:
            try:
                jobs.append(self._reconstitute_job(row.job_state))
            except BaseException:
                self._logger.exception('Unable to restore job "%s" -- removing it', row.id)
                failed_job_ids.add(row.id)

        # Remove all the jobs we failed to restore
        if failed_job_ids:
            delete = self.job_model.delete().where(self.job_model.id.in_(failed_job_ids))
            delete.execute()

        return jobs

    def __repr__(self):
        return '<%s (database=%s)>' % (self.__class__.__name__, self.database.database)
