"""
Stores jobs in a RethinkDB database.
"""
import logging

from apscheduler.jobstores.base import JobStore
from apscheduler.job import Job

try:
    import cPickle as pickle
except ImportError:  # pragma: nocover
    import pickle

try:
    import rethinkdb as r
except ImportError:  # pragma: nocover
    raise ImportError('RethinkDBJobStore requires rethinkdb installed')

logger = logging.getLogger(__name__)


class RethinkDBJobStore(JobStore):
    def __init__(self, database='apscheduler', table='jobs',
                 pickle_protocol=pickle.HIGHEST_PROTOCOL,
                 **connect_args):
        self.jobs = []
        self.pickle_protocol = pickle_protocol

        if not database:
            raise ValueError('The "database" parameter must not be empty')
        if not table:
            raise ValueError('The "table" parameter must not be empty')

        self.conn = r.connect(db=database, **connect_args)
        if not database in r.db_list().run(self.conn):
            r.db_create(database).run(self.conn)
        if not table in r.table_list().run(self.conn):
            r.table_create(table).run(self.conn)

        self.table = r.db(database).table(table)

    def add_job(self, job):
        job_dict = job.__getstate__()
        job_dict['args'] = (
            pickle
            .dumps(job.args, self.pickle_protocol)
            .encode("zip")
            .encode("base64")
            .strip()
        )
        job_dict['trigger'] = (
            pickle
            .dumps(job.trigger, self.pickle_protocol)
            .encode("zip")
            .encode("base64")
            .strip()
        )
        job_dict['kwargs'] = (
            pickle
            .dumps(job.kwargs, self.pickle_protocol)
            .encode("zip")
            .encode("base64")
            .strip()
        )
        job_dict['next_run_time'] = r.epoch_time(
            float(job_dict['next_run_time'].strftime('%s'))
        )
        job_results = self.table.insert(job_dict).run(self.conn)
        job.id = job_results['generated_keys'].pop()
        self.jobs.append(job)

    def remove_job(self, job):
        self.table.get(job.id).delete().run(self.conn)
        self.jobs.remove(job)

    def load_jobs(self):
        jobs = []
        for job_dict in self.table.run(self.conn):
            try:
                job = Job.__new__(Job)
                job_dict['id'] = job_dict.pop('id')
                job_dict['trigger'] = (
                    pickle
                    .loads(
                        job_dict['trigger'].decode("base64").decode("zip")
                    )
                )
                job_dict['args'] = (
                    pickle
                    .loads(
                        job_dict['args'].decode("base64").decode("zip")
                    )
                )
                job_dict['kwargs'] = (
                    pickle
                    .loads(
                        job_dict['kwargs'].decode("base64").decode("zip")
                    )
                )
                job.__setstate__(job_dict)
                jobs.append(job)
            except Exception:
                job_name = job_dict.get('name', '(unknown)')
                logger.exception('Unable to restore job "%s"', job_name)
        self.jobs = jobs

    def update_job(self, job):
        next_run_time = r.epoch_time(
            float(job.next_run_time.strftime('%s'))
        )
        document = {'next_run_time': next_run_time, 'runs': 1}
        self.table.get(job.id).update(document).run(self.conn)

    def close(self):
        self.conn.close()

    def __repr__(self):
        connection = self.conn
        return '<%s (connection=%s)>' % (self.__class__.__name__, connection)
