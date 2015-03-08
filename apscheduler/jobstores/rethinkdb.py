from __future__ import absolute_import

from apscheduler.jobstores.base import BaseJobStore, JobLookupError, ConflictingIdError
from apscheduler.util import maybe_ref, datetime_to_utc_timestamp, utc_timestamp_to_datetime
from apscheduler.job import Job

try:
    import cPickle as pickle
except ImportError:  # pragma: nocover
    import pickle

try:
    import rethinkdb as r
except ImportError:  # pragma: nocover
    raise ImportError('RethinkDBJobStore requires rethinkdb installed')


class RethinkDBJobStore(BaseJobStore):
    """
    Stores jobs in a RethinkDB database. Any leftover keyword arguments are directly passed to rethink's
    `RethinkdbClient <http://www.rethinkdb.com/api/#connect>`_.

    Plugin alias: ``rethinkdb``

    :param str database: database to store jobs in
    :param str collection: collection to store jobs in
    :param client: a :class:`rethinkdb.net.Connection` instance to use instead of providing connection
                   arguments
    :param int pickle_protocol: pickle protocol level to use (for serialization), defaults to the highest available
    """

    def __init__(self, database='apscheduler', table='jobs', client=None,
                 pickle_protocol=pickle.HIGHEST_PROTOCOL, **connect_args):
        super(RethinkDBJobStore, self).__init__()
        self.pickle_protocol = pickle_protocol

        if not database:
            raise ValueError('The "database" parameter must not be empty')
        if not table:
            raise ValueError('The "table" parameter must not be empty')

        if client:
            self.conn = maybe_ref(client)
        else:
            self.conn = r.connect(db=database, **connect_args)

        if database not in r.db_list().run(self.conn):
            r.db_create(database).run(self.conn)

        if table not in r.table_list().run(self.conn):
            r.table_create(table).run(self.conn)

        if 'next_run_time' not in r.table(table).index_list().run(self.conn):
            r.table(table).index_create('next_run_time').run(self.conn)

        self.r = r
        self.table = r.db(database).table(table)

    def lookup_job(self, job_id):
        if job_id:
            document = list(
                self.table.get_all(job_id).pluck('job_state').run(self.conn)
            )
            if document:
                document = document[0]
        else:
            document = None

        return self._reconstitute_job(document['job_state']) if document else None

    def get_due_jobs(self, now):
        if now:
            timestamp = datetime_to_utc_timestamp(now)
            search = (lambda x: x['next_run_time'] <= timestamp)
        else:
            search = None

        return self._get_jobs(search)

    def get_next_run_time(self):
        document = list(
            self.table
            .filter(
                lambda x:
                x['next_run_time'] != None
            )
            .order_by(r.asc('next_run_time'))
            .map(lambda x: x['next_run_time'])
            .limit(1)
            .run(self.conn)
        )
        if document:
            document = utc_timestamp_to_datetime(document[0])
        else:
            document = None

        return document

    def get_all_jobs(self):
        jobs = self._get_jobs()
        self._fix_paused_jobs_sorting(jobs)
        return jobs

    def add_job(self, job):
        if isinstance(job, Job):
            if job.id:
                job_exist = list(self.table.get_all(job.id).run(self.conn))
                if job_exist:
                    job_exist = job_exist[0]
            else:
                job_exist = None
        else:
            job_exist = None

        if not job_exist:
            job_dict = {}
            job_dict['id'] = job.id
            job_dict['job_state'] = (
                pickle
                .dumps(job.__getstate__(), self.pickle_protocol)
                .encode("zip")
                .encode("base64")
                .strip()
            )
            job_dict['next_run_time'] = (
                datetime_to_utc_timestamp(job.next_run_time)
            )

            results = self.table.insert(job_dict).run(self.conn)
            if results['errors'] > 0:
                raise ConflictingIdError(job.id)
        else:
            raise ConflictingIdError(job)

    def update_job(self, job):
        document = {}
        if isinstance(job, Job):
            next_run_time = (
                datetime_to_utc_timestamp(job.next_run_time)
            )
            document['job_state'] = (
                pickle
                .dumps(job.__getstate__(), self.pickle_protocol)
                .encode("zip")
                .encode("base64")
                .strip()
            )
            document['next_run_time'] = next_run_time
            results = self.table.get_all(job.id).update(document).run(self.conn)
            skipped = False in map(lambda x: results[x] == 0, results.keys())
            if results['skipped'] > 0 or results['errors'] > 0 or not skipped:
                raise JobLookupError(job.id)
        else:
            raise JobLookupError(job.id)

    def remove_job(self, job_id):
        if job_id:
            results = self.table.get_all(job_id).delete().run(self.conn)
            skipped = False in map(lambda x: results[x] == 0, results.keys())
            if results['skipped'] > 0 or results['errors'] > 0 or not skipped:
                raise JobLookupError(job_id)
        else:
            raise JobLookupError(job_id)

    def remove_all_jobs(self):
        self.table.delete().run(self.conn)

    def shutdown(self):
        self.conn.close()

    def _reconstitute_job(self, job_state):
        job_state = pickle.loads(job_state.decode("base64").decode("zip"))
        job = Job.__new__(Job)
        job.__setstate__(job_state)
        job._scheduler = self._scheduler
        job._jobstore_alias = self._alias
        return job

    def _get_jobs(self, conditions=None):
        jobs = []
        failed_job_ids = []
        if conditions:
            documents = list(
                self.table
                .filter(
                    lambda x:
                    x['next_run_time'] != None
                )
                .filter(conditions)
                .order_by(r.asc('next_run_time'), 'id')
                .pluck('id', 'job_state')
                .run(self.conn)
            )
        else:
            documents = list(
                self.table
                .order_by(r.asc('next_run_time'), 'id')
                .pluck('id', 'job_state')
                .run(self.conn)
            )
        for document in documents:
            try:
                jobs.append(self._reconstitute_job(document['job_state']))
            except:
                self._logger.exception('Unable to restore job "%s" -- removing it', document['id'])
                failed_job_ids.append(document['id'])

        # Remove all the jobs we failed to restore
        if failed_job_ids:
            r.expr(failed_job_ids).for_each(
                lambda job_id:
                self.table.get_all(job_id).delete()
            ).run(self.conn)

        return jobs

    def __repr__(self):
        connection = self.conn
        return '<%s (connection=%s)>' % (self.__class__.__name__, connection)
