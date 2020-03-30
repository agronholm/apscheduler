import pickle

from apscheduler.jobstores.base import BaseJobStore, JobLookupError, ConflictingIdError
from apscheduler.util import datetime_to_utc_timestamp, utc_timestamp_to_datetime
from apscheduler.job import Job
import sqlite3


class SQLiteJobStore(BaseJobStore):
    """
    Stores jobs in a database table using SQLite3.
    The table will be created if it doesn't exist in the database.

    Plugin alias: ``sqlite``

    :param str db_path: Path to the database or `:memory:` if it should be stored in RAM
    :param str tablename: name of the table to store jobs in
    :param int pickle_protocol: pickle protocol level to use (for serialization), defaults to the
        highest available
    """

    def __init__(self, db_path=':memory:', tablename='apscheduler_jobs',
                 pickle_protocol=pickle.HIGHEST_PROTOCOL):
        super().__init__()
        self.pickle_protocol = pickle_protocol
        self.tablename = tablename
        self.db_path = db_path

    def start(self, scheduler, alias):
        super().start(scheduler, alias)
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute("CREATE TABLE IF NOT EXISTS " + self.tablename +
                       """(
                           id TEXT NOT NULL PRIMARY KEY,
                           next_run_time REAL,
                           job_state BLOB NOT NULL
                           )""")
        cursor.execute("CREATE INDEX next_run_time_index ON " +
                       self.tablename + " (next_run_time)")
        conn.close()

    def lookup_job(self, job_id):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT job_state FROM " + self.tablename +
                       " WHERE id=:job_id", {'job_id': job_id})
        job_state = cursor.fetchone()
        conn.close()
        return self._reconstitute_job(job_state[0]) if job_state else None

    def get_due_jobs(self, now):
        timestamp = datetime_to_utc_timestamp(now)
        return self._get_jobs("next_run_time <= " + str(timestamp))

    def get_next_run_time(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT next_run_time FROM " + self.tablename +
                       " WHERE next_run_time IS NOT NULL " +
                       " ORDER BY next_run_time ASC " +
                       " LIMIT 1")
        next_run_time = cursor.fetchone()
        conn.close()
        return utc_timestamp_to_datetime(next_run_time[0]) if next_run_time else None

    def get_all_jobs(self):
        jobs = self._get_jobs()
        self._fix_paused_jobs_sorting(jobs)
        return jobs

    def add_job(self, job):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            with conn:
                cursor.execute("INSERT INTO " + self.tablename +
                               " VALUES (:id, :next_run_time, :job_state)",
                               {'id': job.id,
                                'next_run_time': datetime_to_utc_timestamp(job.next_run_time),
                                'job_state': pickle.dumps(job.__getstate__(), self.pickle_protocol)
                                })
            conn.close()
        except sqlite3.IntegrityError:
            conn.close()
            raise ConflictingIdError(job.id)

    def update_job(self, job):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        with conn:
            job_next_run_time = datetime_to_utc_timestamp(job.next_run_time)
            updated_rows_count = cursor.execute("UPDATE " + self.tablename +
                                                " SET next_run_time = :next_run_time," +
                                                " job_state = :job_state" +
                                                " WHERE id = :id",
                                                {'id': job.id,
                                                 'next_run_time': job_next_run_time,
                                                 'job_state': pickle.dumps(job.__getstate__(),
                                                                           self.pickle_protocol)
                                                 }).rowcount
        conn.close()
        if updated_rows_count == 0:
            raise JobLookupError(job.id)

    def remove_job(self, job_id):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        with conn:
            deleted_rows_count = cursor.execute("DELETE FROM " + self.tablename +
                                                " WHERE id = :id",
                                                {'id': job_id}).rowcount
        conn.close()
        if deleted_rows_count == 0:
            raise JobLookupError(job_id)

    def remove_all_jobs(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        with conn:
            cursor.execute("""DELETE FROM """ + self.tablename)
        conn.close()

    def shutdown(self):
        pass

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

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT id, job_state FROM " + self.tablename + " " +
                       conditions + " ORDER BY next_run_time ASC")
        failed_job_ids = []
        for row in cursor.fetchall():
            try:
                job_state = row[1]
                jobs.append(self._reconstitute_job(job_state))
            except BaseException:
                job_id = row[0]
                self._logger.exception('Unable to restore job "%s" -- removing it', job_id)
                failed_job_ids.append(job_id)

        # Remove all the jobs we failed to restore
        if failed_job_ids:
            failed_job_ids_dicts = map(lambda x: {'id': x}, failed_job_ids)
            with conn:
                cursor.executemany("DELETE FROM " + self.tablename + " WHERE id = :id",
                                   failed_job_ids_dicts)
        conn.close()
        return jobs

    def __repr__(self):
        return '<%s (db_path=%s)>' % (self.__class__.__name__, self.db_path)
