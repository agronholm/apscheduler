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
    Stores jobs in a RethinkDB database. Any leftover keyword arguments are directly passed to pymongo's `RethinkdbClient
    <http://www.rethinkdb.com/api/#connect>`_.

    Plugin alias: ``rethinkdb``

    :param str database: database to store jobs in
    :param str collection: collection to store jobs in
    :param client: a :class:`rethinkdb.net.Connectio` instance to use instead of providing connection
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

        if not database in r.db_list().run(self.conn):
            r.db_create(database).run(self.conn)

        if not table in r.table_list().run(self.conn):
            r.table_create(table).run(self.conn)

        if not 'next_run_time' in r.table(table).index_list().run(self.conn):
            r.table(table).index_create('next_run_time').run(self.conn)

        self.table = r.db(database).table(table)

    def lookup_job(self, job_id):
        document = self.table.get(job_id).pluck('job_state').run(self.conn)
        return self._reconstitute_job(document['job_state']) if document else None

    def get_due_jobs(self, now):
        timestamp = r.epoch_time(datetime_to_utc_timestamp(now))
        search = (lambda x: x['next_run_time'] <= timestamp)
        return self._get_jobs(search)

    def get_next_run_time(self):
        document = (
            self.table
            .filter(
                lambda x:
                x['next_run_time'] != None
            )
            .pluck('next_run_time')
            .order_by(r.asc('next_run_time'))
            .limit(1)
            .run(self.conn)
        )
        if document:
            document = document[0]['next_run_time']
        else:
            document = None

        return document

    def get_all_jobs(self):
        return self._get_jobs()


    def add_job(self, job):
        job_exist = self.table.get(job.id).run(self.conn)
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
            job_dict['next_run_time'] = r.epoch_time(
                float(job_dict['next_run_time'].strftime('%s'))
            )
            self.table.insert(job_dict).run(self.conn)
        else:
            raise ConflictingIdError(job.id)


    def update_job(self, job):
        document = {}
        next_run_time = r.epoch_time(
            float(job.next_run_time.strftime('%s'))
        )
        document['job_state'] = (
            pickle
            .dumps(job.__getstate__(), self.pickle_protocol)
            .encode("zip")
            .encode("base64")
            .strip()
        )
        document['next_run_time'] = next_run_time
        results = self.table.get(job.id).update(document).run(self.conn)
        if results['skipped'] > 0:
            raise JobLookupError(id)

    def remove_job(self, job):
        results = self.table.get(job.id).delete().run(self.conn)
        if results['deleted'] == 0:
            raise JobLookupError(id)

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
            documents = (
                self.table
                .filter(conditions)
                .pluck('id', 'job_state')
                .order_by(r.asc('next_run_time'))
                .run(self.conn)
            )
        else:
            documents = (
                self.table
                .pluck('id', 'job_state')
                .order_by(r.asc('next_run_time'))
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
                self.table.get(job_id).delete()
            ).run(self.conn)

        return jobs

    def __repr__(self):
        connection = self.conn
        return '<%s (connection=%s)>' % (self.__class__.__name__, connection)
