"""
Stores jobs in a MongoDB database.
"""
from __future__ import absolute_import
import logging

import six

from apscheduler.jobstores.base import BaseJobStore, JobLookupError, ConflictingIdError, TransientJobError
from apscheduler.util import maybe_ref, datetime_to_utc_timestamp, utc_timestamp_to_datetime
from apscheduler.job import Job

try:
    import cPickle as pickle
except ImportError:  # pragma: nocover
    import pickle

try:
    from bson.binary import Binary
    from pymongo.errors import DuplicateKeyError
    from pymongo import Connection, ASCENDING
except ImportError:  # pragma: nocover
    raise ImportError('MongoDBJobStore requires PyMongo installed')

logger = logging.getLogger(__name__)


class MongoDBJobStore(BaseJobStore):
    def __init__(self, scheduler, database='apscheduler', collection='jobs', connection=None,
                 pickle_protocol=pickle.HIGHEST_PROTOCOL, **connect_args):
        self.scheduler = scheduler
        super(MongoDBJobStore, self).__init__()
        self.pickle_protocol = pickle_protocol

        if not database:
            raise ValueError('The "database" parameter must not be empty')
        if not collection:
            raise ValueError('The "collection" parameter must not be empty')

        if connection:
            self.connection = maybe_ref(connection)
        else:
            connect_args.setdefault('w', 1)
            self.connection = Connection(**connect_args)

        self.collection = self.connection[database][collection]
        self.collection.ensure_index('next_run_time', sparse=True)

    def lookup_job(self, id):
        job_dict = self.collection.find_one(id)
        if job_dict is None:
            raise JobLookupError(id)
        return self._reconstitute_job(job_dict)

    def get_pending_jobs(self, now):
        timestamp = datetime_to_utc_timestamp(now)
        jobs = self._get_jobs({'next_run_time': {'$lte': timestamp}})
        next_job_dict = self.collection.find_one({'next_run_time': {'$gt': timestamp}}, fields=['next_run_time'],
                                                 sort=[('next_run_time', ASCENDING)])
        next_run_time = next_job_dict['next_run_time'] if next_job_dict else None
        return jobs, utc_timestamp_to_datetime(next_run_time)

    def get_all_jobs(self):
        return self._get_jobs({})

    def add_job(self, job):
        if not job.func_ref:
            raise TransientJobError(job.id)

        job_dict = job.__getstate__()
        document = {
            '_id': job_dict.pop('id'),
            'next_run_time': datetime_to_utc_timestamp(job_dict['next_run_time']),
            'job_data': Binary(pickle.dumps(job_dict, self.pickle_protocol))
        }
        print('adding job with next run time: %s (timestamp: %s)' % (job_dict['next_run_time'], utc_timestamp_to_datetime(document['next_run_time'])))
        try:
            self.collection.insert(document)
        except DuplicateKeyError:
            raise ConflictingIdError(job.id)

    def update_job(self, id, changes):
        document = self.collection.find_one(id)
        if document is None:
            raise JobLookupError(id)

        if 'id' in changes:
            # The id_ field cannot be updated, so the document must be reinserted instead
            _id = changes.pop('id')
            job_data = pickle.loads(document['job_data'])
            job_data.update(changes)
            document = {
                '_id': _id,
                'next_run_time': datetime_to_utc_timestamp(job_data['next_run_time']),
                'job_data': Binary(pickle.dumps(job_data, self.pickle_protocol))
            }
            try:
                self.collection.insert(document)
            except DuplicateKeyError:
                raise ConflictingIdError(_id)
            self.collection.remove(id)
        elif changes:
            job_data = pickle.loads(document['job_data'])
            job_data.update(changes)
            document_changes = {'job_data': Binary(pickle.dumps(job_data, self.pickle_protocol))}
            if 'next_run_time' in changes:
                document_changes['next_run_time'] = datetime_to_utc_timestamp(changes['next_run_time'])
            self.collection.update({'_id': id}, {'$set': document_changes})

    def remove_job(self, id):
        result = self.collection.remove(id)
        if result and result['n'] != 1:
            raise JobLookupError(id)

    def remove_all_jobs(self):
        self.collection.remove()

    def close(self):
        self.connection.disconnect()

    def _reconstitute_job(self, document):
        job = Job.__new__(Job)
        job_data = pickle.loads(document.pop('job_data'))
        document.update(job_data)
        document['id'] = document['_id']
        job.__setstate__(document)
        return job

    def _get_jobs(self, conditions):
        jobs = []
        for job_dict in self.collection.find(conditions, sort=[('next_run_time', ASCENDING)]):
            try:
                job = self._reconstitute_job(job_dict)
                jobs.append(job)
            except Exception as e:
                logger.exception(six.u('Unable to restore job (id=%s)'), job_dict['_id'])

        return jobs

    def __repr__(self):
        return '<%s (connection=%s)>' % (self.__class__.__name__, self.connection)
