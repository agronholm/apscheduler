from __future__ import absolute_import
import warnings
import six

from apscheduler.jobstores.base import BaseJobStore, JobLookupError, ConflictingIdError
from apscheduler.util import maybe_ref, datetime_to_utc_timestamp, utc_timestamp_to_datetime
from apscheduler.job import Job

try:
    import cPickle as pickle
except ImportError:  # pragma: nocover
    import pickle

try:
    from bson.binary import Binary
    from pymongo.errors import DuplicateKeyError
    from pymongo import MongoClient, ASCENDING
except ImportError:  # pragma: nocover
    raise ImportError('MongoDBJobStore requires PyMongo installed')


class MongoDBJobStore(BaseJobStore):
    """
    Stores jobs in a MongoDB database. Any leftover keyword arguments are directly passed to
    pymongo's `MongoClient
    <http://api.mongodb.org/python/current/api/pymongo/mongo_client.html#pymongo.mongo_client.MongoClient>`_.

    Plugin alias: ``mongodb``

    :param str database: database to store jobs in
    :param str collection: collection to store jobs in
    :param client: a :class:`~pymongo.mongo_client.MongoClient` instance to use instead of
        providing connection arguments
    :param int pickle_protocol: pickle protocol level to use (for serialization), defaults to the
        highest available
    """

    def __init__(self, database='apscheduler', collection='jobs', client=None,
                 pickle_protocol=pickle.HIGHEST_PROTOCOL, **connect_args):
        super(MongoDBJobStore, self).__init__()
        self.pickle_protocol = pickle_protocol

        if not database:
            raise ValueError('The "database" parameter must not be empty')
        if not collection:
            raise ValueError('The "collection" parameter must not be empty')

        if client:
            self.client = maybe_ref(client)
        else:
            connect_args.setdefault('w', 1)
            self.client = MongoClient(**connect_args)

        self.collection = self.client[database][collection]
        self.job_submission_collection = self.client[database]['job_submissions']

    def start(self, scheduler, alias):
        super(MongoDBJobStore, self).start(scheduler, alias)
        self.collection.ensure_index('next_run_time', sparse=True)

    @property
    def connection(self):
        warnings.warn('The "connection" member is deprecated -- use "client" instead',
                      DeprecationWarning)
        return self.client

    def lookup_job(self, job_id):
        document = self.collection.find_one(job_id, ['job_state'])
        return self._reconstitute_job(document['job_state']) if document else None

    def get_due_jobs(self, now):
        timestamp = datetime_to_utc_timestamp(now)
        return self._get_jobs({'next_run_time': {'$lte': timestamp}})

    def get_next_run_time(self):
        document = self.collection.find_one({'next_run_time': {'$ne': None}},
                                            projection=['next_run_time'],
                                            sort=[('next_run_time', ASCENDING)])
        return utc_timestamp_to_datetime(document['next_run_time']) if document else None
    
    def add_job_submission(self, job, now):
        job_submission = {
            'state': 'submitted',
            # TODO: Pickle the 'job.func' so we can recover from 2 diff sessions
            'func': job.func if isinstance(job.func, six.string_types) else job.func.__name__,
            'submitted_at': now,
            'apscheduler_job_id': job.id,
        }
        self.job_submission_collection.insert(job_submission)
        # 'job_submission' passed by reference, gets updated with '_id'
        return job_submission['_id']

    def update_job_submission(self, job_submission_id, **kwargs):
        result = self.job_submission_collection.\
             update({'_id': job_submission_id}, {'$set': kwargs})
        if result and result['n'] == 0:
            raise JobSubmissionLookupError(job_submission_id)
 
    def update_job_submissions(self, conditions, **kwargs):
        query = {'$and': []}
        for column in conditions:
            query['$and'].append({column: conditions[column] })
        result = self.job_submission_collection.update_many(query, {'$set': kwargs})
        num_updates = result.modified_count

        self._logger.info("Updated '{0}' rows where '{1}'...set values to: '{2}'"
                .format(str(num_updates), str(query), str(kwargs)))
    def get_job_submissions_with_states(self, states=[]):
        job_submissions = []
        if states:
            for document in self.job_submission_collection.find({'state': {'$in': states}}):
                obj = dict(document)
                # Replace '_id' with 'id' column name to maintain abstraction
                obj['id'] = obj['_id']
                del obj['_id']
                job_submissions.append(obj)
        else:
            for document in self.job_submission_collection.find({}):
                obj = dict(document)
                obj['id'] = obj['_id']
                del obj['_id']
                job_submissions.append(obj)
        return job_submissions
   
    def get_job_submission(self, job_submission_id):
        js = self.job_submission_collection.find_one(job_submission_id)
        if js:
            _id = js['_id']
            js.update({'id': _id})
            del js['_id']
            return js
        else:
            return None

    def get_all_jobs(self):
        jobs = self._get_jobs({})
        self._fix_paused_jobs_sorting(jobs)
        return jobs

    def add_job(self, job):
        try:
            self.collection.insert({
                '_id': job.id,
                'next_run_time': datetime_to_utc_timestamp(job.next_run_time),
                'job_state': Binary(pickle.dumps(job.__getstate__(), self.pickle_protocol))
            })
        except DuplicateKeyError:
            raise ConflictingIdError(job.id)

    def update_job(self, job):
        changes = {
            'next_run_time': datetime_to_utc_timestamp(job.next_run_time),
            'job_state': Binary(pickle.dumps(job.__getstate__(), self.pickle_protocol))
        }
        result = self.collection.update({'_id': job.id}, {'$set': changes})
        if result and result['n'] == 0:
            raise JobLookupError(job.id)

    def remove_job(self, job_id):
        result = self.collection.remove(job_id)
        if result and result['n'] == 0:
            raise JobLookupError(job_id)

    def remove_all_jobs(self):
        self.collection.remove()

    def shutdown(self):
        self.client.close()

    def _reconstitute_job(self, job_state):
        job_state = pickle.loads(job_state)
        job = Job.__new__(Job)
        job.__setstate__(job_state)
        job._scheduler = self._scheduler
        job._jobstore_alias = self._alias
        return job

    def _get_jobs(self, conditions):
        jobs = []
        failed_job_ids = []
        for document in self.collection.find(conditions, ['_id', 'job_state'],
                                             sort=[('next_run_time', ASCENDING)]):
            try:
                jobs.append(self._reconstitute_job(document['job_state']))
            except:
                self._logger.exception('Unable to restore job "%s" -- removing it',
                                       document['_id'])
                failed_job_ids.append(document['_id'])

        # Remove all the jobs we failed to restore
        if failed_job_ids:
            self.collection.remove({'_id': {'$in': failed_job_ids}})

        return jobs

    def __repr__(self):
        return '<%s (client=%s)>' % (self.__class__.__name__, self.client)
