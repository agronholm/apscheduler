"""
Stores jobs in a Redis database.
"""
from __future__ import absolute_import

import six

from apscheduler.jobstores.base import BaseJobStore, JobLookupError, ConflictingIdError
from apscheduler.util import datetime_to_utc_timestamp, utc_timestamp_to_datetime
from apscheduler.job import Job

try:
    import cPickle as pickle
except ImportError:  # pragma: nocover
    import pickle

try:
    from redis import StrictRedis
except ImportError:  # pragma: nocover
    raise ImportError('RedisJobStore requires redis installed')


class RedisJobStore(BaseJobStore):
    def __init__(self, db=0, jobs_key='apscheduler.jobs', run_times_key='apscheduler.run_times',
                 pickle_protocol=pickle.HIGHEST_PROTOCOL, **connect_args):
        super(RedisJobStore, self).__init__()

        if db is None:
            raise ValueError('The "db" parameter must not be empty')
        if not jobs_key:
            raise ValueError('The "jobs_key" parameter must not be empty')
        if not run_times_key:
            raise ValueError('The "run_times_key" parameter must not be empty')

        self.pickle_protocol = pickle_protocol
        self.jobs_key = jobs_key
        self.run_times_key = run_times_key
        self.redis = StrictRedis(db=int(db), **connect_args)

    def lookup_job(self, job_id):
        job_state = self.redis.hget(self.jobs_key, job_id)
        if job_state is None:
            raise JobLookupError(job_id)

        return self._reconstitute_job(job_state)

    def get_pending_jobs(self, now):
        timestamp = datetime_to_utc_timestamp(now)
        job_ids = self.redis.zrangebyscore(self.run_times_key, 0, timestamp)
        if job_ids:
            job_states = self.redis.hmget(self.jobs_key, *job_ids)
            return self._reconstitute_jobs(six.moves.zip(job_ids, job_states))
        return []

    def get_next_run_time(self):
        next_run_time = self.redis.zrange(self.run_times_key, 0, 0, withscores=True)
        if next_run_time:
            return utc_timestamp_to_datetime(next_run_time[0][1])

    def get_all_jobs(self):
        job_states = self.redis.hgetall(self.jobs_key)
        jobs = self._reconstitute_jobs(six.iteritems(job_states))
        return sorted(jobs, key=lambda job: job.next_run_time)

    def add_job(self, job):
        if self.redis.hexists(self.jobs_key, job.id):
            raise ConflictingIdError(job.id)

        with self.redis.pipeline() as pipe:
            pipe.multi()
            pipe.hset(self.jobs_key, job.id, pickle.dumps(job.__getstate__(), self.pickle_protocol))
            pipe.zadd(self.run_times_key, datetime_to_utc_timestamp(job.next_run_time), job.id)
            pipe.execute()

    def update_job(self, job):
        if not self.redis.hexists(self.jobs_key, job.id):
            raise JobLookupError(job.id)

        with self.redis.pipeline() as pipe:
            pipe.hset(self.jobs_key, job.id, pickle.dumps(job.__getstate__(), self.pickle_protocol))
            if job.next_run_time:
                pipe.zadd(self.run_times_key, datetime_to_utc_timestamp(job.next_run_time), job.id)
            else:
                pipe.zrem(self.run_times_key, job.id)
            pipe.execute()

    def remove_job(self, job_id):
        if not self.redis.hexists(self.jobs_key, job_id):
            raise JobLookupError(job_id)

        with self.redis.pipeline() as pipe:
            pipe.hdel(self.jobs_key, job_id)
            pipe.zrem(self.run_times_key, job_id)
            pipe.execute()

    def remove_all_jobs(self):
        with self.redis.pipeline() as pipe:
            pipe.delete(self.jobs_key)
            pipe.delete(self.run_times_key)
            pipe.execute()

    def shutdown(self):
        self.redis.connection_pool.disconnect()

    @staticmethod
    def _reconstitute_job(job_state):
        job_state = pickle.loads(job_state)
        job = Job.__new__(Job)
        job.__setstate__(job_state)
        return job

    def _reconstitute_jobs(self, job_states):
        jobs = []
        for job_id, job_state in job_states:
            try:
                jobs.append(self._reconstitute_job(job_state))
            except Exception:
                self._logger.exception(six.u('Unable to restore job (id=%s)'), job_id)

        return jobs

    def __repr__(self):
        return '<%s>' % self.__class__.__name__
