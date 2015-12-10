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
    Stores jobs in a RethinkDB database. Any leftover keyword arguments are directly passed to
    rethinkdb's `RethinkdbClient <http://www.rethinkdb.com/api/#connect>`_.

    Plugin alias: ``rethinkdb``

    :param str database: database to store jobs in
    :param str collection: collection to store jobs in
    :param client: a :class:`rethinkdb.net.Connection` instance to use instead of providing
        connection arguments
    :param int pickle_protocol: pickle protocol level to use (for serialization), defaults to the
        highest available
    """

    def __init__(self, database='apscheduler', table='jobs', client=None,
                 pickle_protocol=pickle.HIGHEST_PROTOCOL, **connect_args):
        super(RethinkDBJobStore, self).__init__()

        if not database:
            raise ValueError('The "database" parameter must not be empty')
        if not table:
            raise ValueError('The "table" parameter must not be empty')

        self.database = database
        self.table = table
        self.client = client
        self.pickle_protocol = pickle_protocol
        self.connect_args = connect_args
        self.conn = None

    def start(self, scheduler, alias):
        super(RethinkDBJobStore, self).start(scheduler, alias)

        if self.client:
            self.conn = maybe_ref(self.client)
        else:
            self.conn = r.connect(db=self.database, **self.connect_args)

        if self.database not in r.db_list().run(self.conn):
            r.db_create(self.database).run(self.conn)

        if self.table not in r.table_list().run(self.conn):
            r.table_create(self.table).run(self.conn)

        if 'next_run_time' not in r.table(self.table).index_list().run(self.conn):
            r.table(self.table).index_create('next_run_time').run(self.conn)

        self.table = r.db(self.database).table(self.table)

    def lookup_job(self, job_id):
        results = list(self.table.get_all(job_id).pluck('job_state').run(self.conn))
        return self._reconstitute_job(results[0]['job_state']) if results else None

    def get_due_jobs(self, now):
        return self._get_jobs(r.row['next_run_time'] <= datetime_to_utc_timestamp(now))

    def get_next_run_time(self):
        results = list(
            self.table
            .filter(r.row['next_run_time'] != None)  # flake8: noqa
            .order_by(r.asc('next_run_time'))
            .map(lambda x: x['next_run_time'])
            .limit(1)
            .run(self.conn)
        )
        return utc_timestamp_to_datetime(results[0]) if results else None

    def get_all_jobs(self):
        jobs = self._get_jobs()
        self._fix_paused_jobs_sorting(jobs)
        return jobs

    def add_job(self, job):
        job_dict = {
            'id': job.id,
            'next_run_time': datetime_to_utc_timestamp(job.next_run_time),
            'job_state': r.binary(pickle.dumps(job.__getstate__(), self.pickle_protocol))
        }
        results = self.table.insert(job_dict).run(self.conn)
        if results['errors'] > 0:
            raise ConflictingIdError(job.id)

    def update_job(self, job):
        changes = {
            'next_run_time': datetime_to_utc_timestamp(job.next_run_time),
            'job_state': r.binary(pickle.dumps(job.__getstate__(), self.pickle_protocol))
        }
        results = self.table.get_all(job.id).update(changes).run(self.conn)
        skipped = False in map(lambda x: results[x] == 0, results.keys())
        if results['skipped'] > 0 or results['errors'] > 0 or not skipped:
            raise JobLookupError(job.id)

    def remove_job(self, job_id):
        results = self.table.get_all(job_id).delete().run(self.conn)
        if results['deleted'] + results['skipped'] != 1:
            raise JobLookupError(job_id)

    def remove_all_jobs(self):
        self.table.delete().run(self.conn)

    def shutdown(self):
        self.conn.close()

    def _reconstitute_job(self, job_state):
        job_state = pickle.loads(job_state)
        job = Job.__new__(Job)
        job.__setstate__(job_state)
        job._scheduler = self._scheduler
        job._jobstore_alias = self._alias
        return job

    def _get_jobs(self, predicate=None):
        jobs = []
        failed_job_ids = []
        query = (self.table.filter(r.row['next_run_time'] != None).filter(predicate) if
                 predicate else self.table)
        query = query.order_by('next_run_time', 'id').pluck('id', 'job_state')

        for document in query.run(self.conn):
            try:
                jobs.append(self._reconstitute_job(document['job_state']))
            except:
                self._logger.exception('Unable to restore job "%s" -- removing it', document['id'])
                failed_job_ids.append(document['id'])

        # Remove all the jobs we failed to restore
        if failed_job_ids:
            r.expr(failed_job_ids).for_each(
                lambda job_id: self.table.get_all(job_id).delete()).run(self.conn)

        return jobs

    def __repr__(self):
        connection = self.conn
        return '<%s (connection=%s)>' % (self.__class__.__name__, connection)
