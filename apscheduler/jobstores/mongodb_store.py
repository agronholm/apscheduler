"""
Stores jobs in a MongoDB database.
"""
from apscheduler.jobstores.base import JobStore
from apscheduler.job import Job
from apscheduler.util import obj_to_ref

try:
    import cPickle as pickle
except ImportError:
    import pickle

try:
    from bson.binary import Binary
    from pymongo.connection import Connection
except ImportError:
    raise ImportError('MongoDBJobStore requires PyMongo installed')


class MongoDBJobStore(JobStore):
    def __init__(self, database='apscheduler', collection='jobs',
                 connection=None, pickle_protocol=pickle.HIGHEST_PROTOCOL,
                 **connect_args):
        self.jobs = []
        self.pickle_protocol = pickle_protocol

        if connection:
            self._connection = connection
        else:
            self._connection = None
            self._connect_args = connect_args

        if not database:
            raise ValueError('The "database" parameter must not be empty')
        if not collection:
            raise ValueError('The "collection" parameter must not be empty')

        self._dbname = database
        self._collname = collection
        self._collection = None

    @property
    def collection(self):
        if (not self._collection or not
            self._collection.database.connection.host):
            if not self._connection:
                self._connection = Connection(**self._connect_args)
            self._collection = self._connection[self._dbname][self._collname]

        return self._collection

    def add_job(self, job):
        job.func_ref = obj_to_ref(job.func)
        job_dict = job.__getstate__()
        job_dict['trigger'] = Binary(pickle.dumps(job.trigger,
                                                  self.pickle_protocol))
        job_dict['args'] = Binary(pickle.dumps(job.args,
                                               self.pickle_protocol))
        job_dict['kwargs'] = Binary(pickle.dumps(job.kwargs,
                                                 self.pickle_protocol))
        job.id = self.collection.insert(job_dict)
        self.jobs.append(job)

    def remove_job(self, job):
        self.collection.remove(job.id)
        self.jobs.remove(job)

    def load_jobs(self):
        jobs = []
        for job_dict in self.collection.find():
            job = Job.__new__(Job)
            job_dict['id'] = job_dict.pop('_id')
            job_dict['trigger'] = pickle.loads(job_dict['trigger'])
            job_dict['args'] = pickle.loads(job_dict['args'])
            job_dict['kwargs'] = pickle.loads(job_dict['kwargs'])
            job.__setstate__(job_dict)
            jobs.append(job)
        self.jobs = jobs

    def update_job(self, job):
        spec = {'_id': job.id}
        document = {'$set': {'next_run_time': job.next_run_time},
                    '$inc': {'runs': 1}}
        self.collection.update(spec, document)

    def __repr__(self):
        connection = self.collection.database.connection
        return '<%s (connection=%s)>' % (self.__class__.__name__, connection)
