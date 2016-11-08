from __future__ import absolute_import
import six

import os
from datetime import datetime
import time

from pytz import utc
from kazoo.exceptions import NoNodeError, NodeExistsError

from apscheduler.jobstores.base import BaseJobStore, JobLookupError, ConflictingIdError
from apscheduler.util import maybe_ref, datetime_to_utc_timestamp, utc_timestamp_to_datetime
from apscheduler.job import Job

try:
    import cPickle as pickle
except ImportError:  # pragma: nocover
    import pickle

try:
    from kazoo.client import KazooClient
except ImportError:  # pragma: nocover
    raise ImportError('ZooKeeperJobStore requires Kazoo installed')


class ZooKeeperJobStore(BaseJobStore):
    """
    Stores jobs in a ZooKeeper tree. Any leftover keyword arguments are directly passed to
    kazoo's `KazooClient
    <http://kazoo.readthedocs.io/en/latest/api/client.html>`_.

    Plugin alias: ``zookeeper``

    :param str path: path to store jobs in
    :param client: a :class:`~kazoo.client.KazooClient` instance to use instead of
        providing connection arguments
    :param int pickle_protocol: pickle protocol level to use (for serialization), defaults to the
        highest available
    """

    def __init__(self, path='/apscheduler', job_submission_path='/apscheduler_job_submissions', client=None, close_connection_on_exit=False,
                 pickle_protocol=pickle.HIGHEST_PROTOCOL, **connect_args):
        super(ZooKeeperJobStore, self).__init__()
        self.pickle_protocol = pickle_protocol
        self.close_connection_on_exit = close_connection_on_exit

        if not path:
            raise ValueError('The "path" parameter must not be empty')

        if not job_submission_path:
            raise ValueError('The "job_submission_path" parameter must not be empty')
        
        self.path = path
        self.job_submission_path = job_submission_path

        if client:
            self.client = maybe_ref(client)
        else:
            self.client = KazooClient(**connect_args)
        self._ensured_path = False

    def _ensure_paths(self):
        if not self._ensured_path:
            self.client.ensure_path(self.path)
            self.client.ensure_path(self.job_submission_path)
        self._ensured_path = True

    def start(self, scheduler, alias):
        super(ZooKeeperJobStore, self).start(scheduler, alias)
        if not self.client.connected:
            self.client.start()

    def lookup_job(self, job_id):
        self._ensure_paths()
        node_path = os.path.join(self.path, job_id)
        try:
            content, _ = self.client.get(node_path)
            doc = pickle.loads(content)
            job = self._reconstitute_job(doc['job_state'])
            return job
        except:
            return None

    def get_due_jobs(self, now):
        timestamp = datetime_to_utc_timestamp(now)
        jobs = [job_def['job'] for job_def in self._get_jobs()
                if job_def['next_run_time'] is not None and job_def['next_run_time'] <= timestamp]
        return jobs

    def get_next_run_time(self):
        next_runs = [job_def['next_run_time'] for job_def in self._get_jobs()
                     if job_def['next_run_time'] is not None]
        return utc_timestamp_to_datetime(min(next_runs)) if len(next_runs) > 0 else None

    def add_job_submission(self, job, now):
        self._ensure_paths()
        utc_now = now.astimezone(utc)
        job_submission_id = job.id + "_" + \
            str((utc_now - utc.localize(datetime(1970, 1, 1))).total_seconds() * 1000 )
        node_path = os.path.join(self.job_submission_path, job_submission_id)
        self._logger.info(node_path)
        value = {
            'state': 'submitted',
            # TODO: Pickle the 'job.func' so we can recover from 2 diff sessions
            'func': job.func if isinstance(job.func, six.string_types) else job.func.__name__,
            'submitted_at': now,
            'apscheduler_job_id': job.id,
        }
        data = pickle.dumps(value, self.pickle_protocol)
        try:
            self.client.create(node_path, value=data)
        except NodeExistsError:
            raise ConflictingIdError(job_submission_id)
        return job_submission_id

    def update_job_submission(self, job_submission_id, **kwargs):
        self._ensure_paths()
        node_path = os.path.join(self.job_submission_path, job_submission_id)
        try:
            # Get current job_submission data
            content, _ = self.client.get(node_path)
            doc = pickle.loads(content)
            # Update the job_submission data
            doc.update(kwargs)    
            data = pickle.dumps(doc, self.pickle_protocol)
            self.client.set(node_path, value=data)
        except NoNodeError:
            raise JobSubmissionLookupError(job_submission_id)

    def update_job_submissions(self, conditions, **kwargs):
        self._ensure_paths()
        job_submissions = self.get_job_submissions_with_states()
        for js in job_submissions:
            node_path = os.path.join(self.job_submission_path, js['id'])
            update_flag = True
            for column in conditions:
                val = conditions[column]
                if js[column] != val:
                    update_flag = False
            if update_flag:
                js.update(kwargs)
                data = pickle.dumps(js, self.pickle_protocol)
                self.client.set(node_path, value=data)
    
    def get_job_submissions_with_states(self, states=[]):
        self._ensure_paths()
        job_submissions = []
        all_ids = self.client.get_children(self.job_submission_path)
        for node_name in all_ids:
            try:
                node_path = os.path.join(self.job_submission_path, node_name)
                content, _ = self.client.get(node_path)
                doc = pickle.loads(content)
                if len(states) == 0 or doc['state'] in states:
                    doc.update({"id": node_name})
                    job_submissions.append(doc)
            except:
                self._logger.exception('Unable to restore job submission "%s"' % node_name)
        return job_submissions

    def get_job_submission(self, job_submission_id):
        self._ensure_paths()
        node_path = os.path.join(self.job_submission_path, job_submission_id)
        try:
            content, _ = self.client.get(node_path)
            doc = pickle.loads(content)
            doc.update({"id": job_submission_id})
            return doc
        except:
            return None

    def get_all_jobs(self):
        jobs = [job_def['job'] for job_def in self._get_jobs()]
        self._fix_paused_jobs_sorting(jobs)
        return jobs

    def add_job(self, job):
        self._ensure_paths()
        node_path = os.path.join(self.path,  str(job.id))
        value = {
            'next_run_time': datetime_to_utc_timestamp(job.next_run_time),
            'job_state': job.__getstate__()
        }
        data = pickle.dumps(value, self.pickle_protocol)
        try:
            self.client.create(node_path, value=data)
        except NodeExistsError:
            raise ConflictingIdError(job.id)

    def update_job(self, job):
        self._ensure_paths()
        node_path = os.path.join(self.path,  str(job.id))
        changes = {
            'next_run_time': datetime_to_utc_timestamp(job.next_run_time),
            'job_state': job.__getstate__()
        }
        data = pickle.dumps(changes, self.pickle_protocol)
        try:
            self.client.set(node_path, value=data)
        except NoNodeError:
            raise JobLookupError(job.id)

    def remove_job(self, job_id):
        self._ensure_paths()
        node_path = os.path.join(self.path,  str(job_id))
        try:
            self.client.delete(node_path)
        except NoNodeError:
            raise JobLookupError(job_id)

    def remove_all_jobs(self):
        try:
            self.client.delete(self.path, recursive=True)
        except NoNodeError:
            pass
        self._ensured_path = False
    
    def remove_all_job_submissions(self):
        try:
            self.client.delete(self.job_submission_path, recursive=True)
        except NoNodeError:
            pass
        self._ensured_path = False

    def shutdown(self):
        if self.close_connection_on_exit:
            self.client.stop()
            self.client.close()

    def _reconstitute_job(self, job_state):
        job_state = job_state
        job = Job.__new__(Job)
        job.__setstate__(job_state)
        job._scheduler = self._scheduler
        job._jobstore_alias = self._alias
        return job

    def _get_jobs(self):
        self._ensure_paths()
        jobs = []
        failed_job_ids = []
        all_ids = self.client.get_children(self.path)
        for node_name in all_ids:
            try:
                node_path = os.path.join(self.path, node_name)
                content, _ = self.client.get(node_path)
                doc = pickle.loads(content)
                job_def = {
                    'job_id': node_name,
                    'next_run_time': doc['next_run_time'] if doc['next_run_time'] else None,
                    'job_state': doc['job_state'],
                    'job': self._reconstitute_job(doc['job_state']),
                    'creation_time': _.ctime
                }
                jobs.append(job_def)
            except:
                self._logger.exception('Unable to restore job "%s" -- removing it' % node_name)
                failed_job_ids.append(node_name)

        # Remove all the jobs we failed to restore
        if failed_job_ids:
            for failed_id in failed_job_ids:
                self.remove_job(failed_id)
        paused_sort_key = datetime(9999, 12, 31, tzinfo=utc)
        return sorted(jobs, key=lambda job_def: (job_def['job'].next_run_time or paused_sort_key,
                                                 job_def['creation_time']))

    def __repr__(self):
        self._logger.exception('<%s (client=%s)>' % (self.__class__.__name__, self.client))
        return '<%s (client=%s)>' % (self.__class__.__name__, self.client)
