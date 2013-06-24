"""
Stores jobs in a Redis database.
"""
from uuid import uuid4
from datetime import datetime
import logging

from apscheduler.jobstores.base import JobStore
from apscheduler.job import Job

try:
    import cPickle as pickle
except ImportError:  # pragma: nocover
    import pickle

try:
    from redis import StrictRedis
except ImportError:  # pragma: nocover
    raise ImportError('RedisJobStore requires redis installed')

try:
    long = long
except NameError:
    long = int

logger = logging.getLogger(__name__)


class RedisJobStore(JobStore):
    def __init__(self, db=0, key_prefix='jobs.',
                 pickle_protocol=pickle.HIGHEST_PROTOCOL, **connect_args):
        self.jobs = []
        self.pickle_protocol = pickle_protocol
        self.key_prefix = key_prefix

        if db is None:
            raise ValueError('The "db" parameter must not be empty')
        if not key_prefix:
            raise ValueError('The "key_prefix" parameter must not be empty')

        self.redis = StrictRedis(db=db, **connect_args)

    def add_job(self, job):
        job.id = str(uuid4())
        job_state = job.__getstate__()
        job_dict = {
            'job_state': pickle.dumps(job_state, self.pickle_protocol),
            'runs': '0',
            'next_run_time': job_state.pop('next_run_time').isoformat()}
        self.redis.hmset(self.key_prefix + job.id, job_dict)
        self.jobs.append(job)

    def remove_job(self, job):
        self.redis.delete(self.key_prefix + job.id)
        self.jobs.remove(job)

    def load_jobs(self):
        jobs = []
        keys = self.redis.keys(self.key_prefix + '*')
        pipeline = self.redis.pipeline()
        for key in keys:
            pipeline.hgetall(key)
        results = pipeline.execute()

        for job_dict in results:
            job_state = {}
            try:
                job = Job.__new__(Job)
                job_state = pickle.loads(job_dict['job_state'.encode()])
                job_state['runs'] = long(job_dict['runs'.encode()])
                dateval = job_dict['next_run_time'.encode()].decode()
                job_state['next_run_time'] = datetime.strptime(
                    dateval, '%Y-%m-%dT%H:%M:%S')
                job.__setstate__(job_state)
                jobs.append(job)
            except Exception:
                job_name = job_state.get('name', '(unknown)')
                logger.exception('Unable to restore job "%s"', job_name)
        self.jobs = jobs

    def update_job(self, job):
        attrs = {
            'next_run_time': job.next_run_time.isoformat(),
            'runs': job.runs}
        self.redis.hmset(self.key_prefix + job.id, attrs)

    def close(self):
        self.redis.connection_pool.disconnect()

    def __repr__(self):
        return '<%s>' % self.__class__.__name__
