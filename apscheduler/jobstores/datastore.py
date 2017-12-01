import datetime
import pickle
import warnings

from apscheduler.job import Job
from apscheduler.jobstores.base import BaseJobStore, ConflictingIdError, JobLookupError

try:
    from gcloud import datastore as ds
except ImportError:
    raise ImportError('GoogleDatastoreStore needs gcloud installed')


class GoogleDatastoreStore(BaseJobStore):
    def __init__(self, key_prefix=None, namespace=None, pickle_protocol=None):
        if key_prefix and not isinstance(key_prefix, ds.Key):
            raise ValueError('GoogleDatastoreStore key_prefix needs to be Key() type')
        self.pickle_protocol = pickle_protocol or pickle.HIGHEST_PROTOCOL
        self.parent_key = key_prefix
        self.client = ds.Client(namespace=namespace)

    def start(self, scheduler, alias):
        super(GoogleDatastoreStore, self).start(scheduler, alias)

    @property
    def connection(self):
        warnings.warn('The "connection" member is deprecated -- use "client" instead',
                      DeprecationWarning)
        return self.client

    def lookup_job(self, job_id):
        doc_key = self._key_from_jobid(job_id)
        job_entity = self.client.get(doc_key)
        return self.job_from_entity(job_entity)

    def get_due_jobs(self, now):
        now = datetime.datetime.utcnow()
        query = self.client.query(
            kind='APScheduler',
            ancestor=self.parent_key,
        )
        query.add_filter('next_run_time', '<=', now)
        return [self._job_from_entity(entity) for entity in query.fetch()]

    def get_next_run_time(self):
        query = self.client.query(
            kind='APScheduler',
            ancestor=self.parent_key,
            projection=('next_run_time',),
            order=('next_run_time',),
        )
        res = query.fetch(limit=1)
        for job in res:
            pass
        else:
            job = {}
        return job.get('next_run_time', None)

    def get_all_jobs(self):
        query = self.client.query(
            kind='APScheduler',
            ancestor=self.parent_key
        )
        jobs = [self._job_from_entity(entity) for entity in query.fetch()]
        self._fix_paused_jobs_sorting(jobs)
        return jobs

    def add_job(self, job):
        entity = self._entity_from_job(job)
        if self.client.get(entity.key):
            raise ConflictingIdError(job.id)
        self.client.put(entity)

    def update_job(self, job):
        entity = self._entity_from_job(job)
        if not self.client.get(entity.key):
            raise JobLookupError(job.id)
        self.client.put(entity)

    def remove_job(self, job_id):
        key = self._key_from_jobid(job_id)
        if not self.client.get(key):
            raise JobLookupError(job_id)
        self.client.delete(key)

    def remove_all_jobs(self):
        entities = self.client.query(kind='APScheduler', ancestor=self.parent_key).fetch()
        while entities:
            self.client.delete_multi([entity.key for entity in entities])
            entities = self.client.query(kind='APScheduler', ancestor=self.parent_key).fetch()
        return

    def shutdown(self):
        pass

    def _key_from_jobid(self, job_id):
        return self.client.key('APScheduler', job_id, parent=self.parent_key)

    def _entity_from_job(self, job):
        job_key = self._key_from_jobid(job.id)
        entity = ds.Entity(key=job_key)
        entity['next_run_time'] = job.next_run_time
        entity['job'] = pickle.dumps(job.__getstate__(), self.pickle_protocol)
        return entity

    def _job_from_entity(self, entity):
        if not entity:
            return None
        job = Job.__new__(Job)
        job.__setstate__(pickle.loads(entity['job']))
        job._scheduler = self._scheduler
        job._jobstore_alias = self._alias
        return job
