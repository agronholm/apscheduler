from __future__ import absolute_import
from datetime import datetime

from pytz import utc
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
    from redis.exceptions import WatchError
except ImportError:  # pragma: nocover
    raise ImportError('RedisJobStore requires redis installed')


class RedisJobStore(BaseJobStore):
    """
    Stores jobs in a Redis database. Any leftover keyword arguments are directly passed to redis's
    :class:`~redis.StrictRedis`.

    Plugin alias: ``redis``

    :param int db: the database number to store jobs in
    :param str jobs_key: key to store jobs in
    :param str job_submissions_key: key to store job_submissions in
    :param str run_times_key: key to store the jobs' run times in
    :param int pickle_protocol: pickle protocol level to use (for serialization), defaults to the
        highest available
    """

    def __init__(self, db=0, jobs_key='apscheduler.jobs',
                 job_submissions_key='apscheduler.job_submissions',
                 run_times_key='apscheduler.run_times', pickle_protocol=pickle.HIGHEST_PROTOCOL,
                 **connect_args):
        super(RedisJobStore, self).__init__()

        if db is None:
            raise ValueError('The "db" parameter must not be empty')
        if not jobs_key:
            raise ValueError('The "jobs_key" parameter must not be empty')
        if not run_times_key:
            raise ValueError('The "run_times_key" parameter must not be empty')

        self.pickle_protocol = pickle_protocol
        self.jobs_key = jobs_key
        self.job_submissions_key = job_submissions_key
        self.run_times_key = run_times_key
        self.redis = StrictRedis(db=int(db), **connect_args)

    def lookup_job(self, job_id):
        job_state = self.redis.hget(self.jobs_key, job_id)
        return self._reconstitute_job(job_state) if job_state else None

    def get_due_jobs(self, now):
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

    def add_job_submission(self, job, now):
        with self.redis.pipeline() as pipe:
            while 1:
                try:
                    job_submission = {
                        'state': 'submitted',
                        'func': job.func if isinstance(job.func, six.string_types)
                                 else job.func.__name__,
                        'submitted_at': now,
                        'apscheduler_job_id': job.id
                    }
                    # Use length of hash to set ID of job_submissions
                    pipe.watch(self.job_submissions_key)
                    current_length = pipe.hlen(self.job_submissions_key)
                    job_submission_id = int(current_length) + 1

                    pipe.multi()
                    pipe.hset(self.job_submissions_key, str(job_submission_id),
                              pickle.dumps(job_submission, self.pickle_protocol))
                    pipe.execute()
                    break
                except WatchError:
                    # This should never happen due to the jobstore lock !
                    self._logger.exception("WatchError was raised in Redis jobstore! Multiple " +
                                           "threads/workers are writing to the jobstore at 1 " +
                                           "time! This shouldn't happen due to the jobstore lock")
                    raise
            return job_submission_id

    def update_job_submissions(self, conditions, **kwargs):
        # Get all jobs that satisfy conditions
        job_submissions_dict = self.redis.hgetall(self.job_submissions_key)
        with self.redis.pipeline() as pipe:
            for key in job_submissions_dict:
                job_sub = pickle.loads(job_submissions_dict[key])
                update_flag = True
                for column in conditions:
                    val = conditions[column]
                    if job_sub[column] != val:
                        update_flag = False
                if update_flag:
                    job_sub.update(kwargs)
                    pipe.hset(self.job_submissions_key, key,
                              pickle.dumps(job_sub, self.pickle_protocol))
            pipe.execute()

    def update_job_submission(self, job_submission_id, **kwargs):
        with self.redis.pipeline() as pipe:
            pipe.hset(self.job_submissions_key, str(job_submission_id),
                      pickle.dumps(kwargs, self.pickle_protocol))
            pipe.execute()

    def get_job_submission(self, job_submission_id):
        pickled_job_submission = self.redis.hget(self.job_submissions_key, str(job_submission_id))
        if not pickled_job_submission:
            return None
        job_sub = pickle.loads(pickled_job_submission)
        job_sub.update({'id': job_submission_id})
        return job_sub

    def get_job_submissions_with_states(self, states=[]):
        job_submissions = []
        job_submissions_dict = self.redis.hgetall(self.job_submissions_key)
        for key in job_submissions_dict:
            job_submission = pickle.loads(job_submissions_dict[key])
            if len(states) == 0 or job_submission['state'] in states:
                job_submission.update({"id": int(key)})
                job_submissions.append(job_submission)
        return job_submissions

    def get_all_jobs(self):
        job_states = self.redis.hgetall(self.jobs_key)
        jobs = self._reconstitute_jobs(six.iteritems(job_states))
        paused_sort_key = datetime(9999, 12, 31, tzinfo=utc)
        return sorted(jobs, key=lambda job: job.next_run_time or paused_sort_key)

    def add_job(self, job):
        if self.redis.hexists(self.jobs_key, job.id):
            raise ConflictingIdError(job.id)

        with self.redis.pipeline() as pipe:
            pipe.multi()
            pipe.hset(self.jobs_key, job.id, pickle.dumps(job.__getstate__(),
                                                          self.pickle_protocol))
            if job.next_run_time:
                pipe.zadd(self.run_times_key, datetime_to_utc_timestamp(job.next_run_time), job.id)
            pipe.execute()

    def update_job(self, job):
        if not self.redis.hexists(self.jobs_key, job.id):
            raise JobLookupError(job.id)

        with self.redis.pipeline() as pipe:
            pipe.hset(self.jobs_key, job.id, pickle.dumps(job.__getstate__(),
                                                          self.pickle_protocol))
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

    def remove_all_job_submissions(self):
        with self.redis.pipeline() as pipe:
            pipe.delete(self.job_submissions_key)
            pipe.execute()

    def shutdown(self):
        self.redis.connection_pool.disconnect()

    def _reconstitute_job(self, job_state):
        job_state = pickle.loads(job_state)
        job = Job.__new__(Job)
        job.__setstate__(job_state)
        job._scheduler = self._scheduler
        job._jobstore_alias = self._alias
        return job

    def _reconstitute_jobs(self, job_states):
        jobs = []
        failed_job_ids = []
        for job_id, job_state in job_states:
            try:
                jobs.append(self._reconstitute_job(job_state))
            except:
                self._logger.exception('Unable to restore job "%s" -- removing it', job_id)
                failed_job_ids.append(job_id)

        # Remove all the jobs we failed to restore
        if failed_job_ids:
            with self.redis.pipeline() as pipe:
                pipe.hdel(self.jobs_key, *failed_job_ids)
                pipe.zrem(self.run_times_key, *failed_job_ids)
                pipe.execute()

        return jobs

    def __repr__(self):
        return '<%s>' % self.__class__.__name__
