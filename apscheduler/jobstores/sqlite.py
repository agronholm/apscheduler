import pickle

from apscheduler.jobstores.base import BaseJobStore, JobLookupError, ConflictingIdError
from apscheduler.util import maybe_ref, datetime_to_utc_timestamp, utc_timestamp_to_datetime
from apscheduler.job import Job
import sqlite3

class SQLiteJobStore(BaseJobStore):
    """
    Stores jobs in a database table using SQLAlchemy.
    The table will be created if it doesn't exist in the database.

    Plugin alias: ``sqlalchemy``

    :param str url: connection string (see
        :ref:`SQLAlchemy documentation <sqlalchemy:database_urls>` on this)
    :param engine: an SQLAlchemy :class:`~sqlalchemy.engine.Engine` to use instead of creating a
        new one based on ``url``
    :param str tablename: name of the table to store jobs in
    :param metadata: a :class:`~sqlalchemy.schema.MetaData` instance to use instead of creating a
        new one
    :param int pickle_protocol: pickle protocol level to use (for serialization), defaults to the
        highest available
    :param str tableschema: name of the (existing) schema in the target database where the table
        should be
    :param dict engine_options: keyword arguments to :func:`~sqlalchemy.create_engine`
        (ignored if ``engine`` is given)
    """

    def __init__(self, url=':memory:', tablename='apscheduler_jobs',
                 pickle_protocol=pickle.HIGHEST_PROTOCOL):
        super().__init__()
        self.pickle_protocol = pickle_protocol
        self.tablename = tablename
        self.url = url

        self.conn = sqlite3.connect(url)
        self.cursor = self.conn.cursor()

        self.cursor.execute("""CREATE TABLE IF NOT EXISTS :tablename (
                        id INTEGER NOT NULL PRIMARY KEY,
                        next_run_time REAL,
                        job_state BLOB NOT NULL
                    )""", {'tablename': tablename}) 
        self.cursor.execute("CREATE INDEX next_run_time_index :tablename (next_run_time)", {'tablename': tablename})

    def start(self, scheduler, alias):
        super().start(scheduler, alias)

    def lookup_job(self, job_id):
        self.cursor.execute("SELECT job_state FROM :tablename WHERE id=:job_id", {'tablename': self.tablename, 'job_id': job_id})
        job_state = self.cursor.fetchone()
        return self._reconstitute_job(job_state) if job_state else None

    def get_due_jobs(self, now):
        timestamp = datetime_to_utc_timestamp(now)
        return self._get_jobs("next_run_time <= " + timestamp)

    def get_next_run_time(self):
        self.cursor.execute("""SELECT TOP 1 next_run_time FROM :tablename
                WHERE next_run_time IS NOT NULL
                ORDER BY next_run_time""")
        next_run_time = self.cursor.fetchone() 
        return utc_timestamp_to_datetime(next_run_time)

    def get_all_jobs(self):
        jobs = self._get_jobs()
        self._fix_paused_jobs_sorting(jobs)
        return jobs

    def add_job(self, job):
        try:
            with self.conn:
                self.cursor.execute("INSERT INTO :tablename VALUES (:id, :next_run_time, :job_state)", {'tablenmae': self.tablename, 'id': job.id, 'next_run_time': datetime_to_utc_timestamp(job.next_run_time), 'job_state': pickle.dumps(job.__getstate__(), self.pickle_protocol)})
        except sqlite3.IntegrityError:
            raise ConflictingIdError(job.id)

    def update_job(self, job):
        try:
            with self.conn:
                self.cursor.execute("""UPADTE :tablename SET next_run_time = :next_run_time, job_state = :job_state WHERE id = :id""", {'tablenmae': self.tablename, 'id': job.id, 'next_run_time': datetime_to_utc_timestamp(job.next_run_time), 'job_state': pickle.dumps(job.__getstate__(), self.pickle_protocol)})
        except sqlite3.Error:
            raise JobLookupError(job.id)

    def remove_job(self, job_id):
        try:
            with self.conn:
                self.cursor.execute("""DELETE from :tablename WHERE id = :id""", {'tablenmae': self.tablename, 'id': job_id})
        except sqlite3.Error:
            raise JobLookupError(job_id)

    def remove_all_jobs(self):
        with self.conn:
            self.cursor.execute("""DELETE from :tablename""", {'tablenmae': self.tablename})

    def shutdown(self):
        self.conn.close()

    def _reconstitute_job(self, job_state):
        job_state = pickle.loads(job_state)
        job_state['jobstore'] = self
        job = Job.__new__(Job)
        job.__setstate__(job_state)
        job._scheduler = self._scheduler
        job._jobstore_alias = self._alias
        return job

    def _get_jobs(self, conditions=""):
        jobs = []
        if conditions != "":
            conditions = " WHERE " + conditions

        self.cursor.execute("SELECT id, job_state FROM :tablename" + conditions, {'tablename': self.tablename})
        failed_job_ids = [] 
        for row in self.cursor.fetchall(): 
            try:
                job_state = row[1]
                jobs.append(self._reconstitute_job(job_state))
            except BaseException:
                id = row[0]
                self._logger.exception('Unable to restore job "%s" -- removing it', id)
                failed_job_ids.append(id)

        # Remove all the jobs we failed to restore
        if failed_job_ids:
            failed_job_ids_dicts = map(lambda x: {'tablename': self.tablename, 'id': x}, failed_job_ids)
            with self.conn:
                self.cursor.executemany("DELETE from :tablename WHERE id = :id", failed_job_id_dicts) 
        return jobs

    def __repr__(self):
        return '<%s (url=%s)>' % (self.__class__.__name__, self.url)
