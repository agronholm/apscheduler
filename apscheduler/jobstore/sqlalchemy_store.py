"""
Stores jobs in a database table using SQLAlchemy.
"""

from apscheduler.jobstore.base import JobStore

try:
    from sqlalchemy import *
except ImportError:
    raise ImportError('SQLAlchemyJobStore requires SQLAlchemy installed')


class SQLAlchemyJobStore(JobStore):
    BINARY_COLUMN_LENGTH = 8192

    stores_persistent = True

    def __init__(self, engine=None, url=None, metadata=None,
                 tablename='apscheduler_jobs'):
        if engine:
            self.engine = engine
        elif url:
            self.engine = create_engine(url)
        else:
            raise ValueError('Need either "engine" or "url" specified!')

        metadata = metadata or MetaData()
        self.jobs_table = self._make_jobs_table(tablename, metadata)
        self.jobs_table.create(self.engine, True)

    def _make_jobs_table(self, name, metadata):
        return Table(name, metadata,
            Column('id', Integer, primary_key=True),
            Column('name', String(1024)),
            Column('job_data', PickleType, nullable=False),
            Column('next_run_time', DateTime, nullable=False))

    def add_job(self, job):
        insert = self.jobs_table.insert().values(name=job.name, job_data=job,
            next_run_time=job.next_run_time)
        result = self.engine.execute(insert)
        job.id = result.inserted_primary_key[0]
        job.jobstore = self

    def update_jobs(self, jobs):
        update = self.jobs_table.update()
        update = update.where(self.jobs_table.c.id == bindparam('_id'))
        update = update.values(name=bindparam('_name'),
                               job_data=bindparam('_job_data'),
                               next_run_time=bindparam('_next_run_time'))

        params = []
        for job in jobs:
            param = dict(_name=job.name, _job_data=job,
                         _next_run_time=job.next_run_time)
            params.append(param)

        self.engine.execute(update, params)

    def remove_jobs(self, jobs):
        job_ids = set(job.id for job in jobs)
        delete = self.jobs_table.delete(self.jobs_table.c.id.in_(job_ids))
        self.engine.execute(delete)

    def get_jobs(self, end_time=None):
        query = select([self.jobs_table.c.id, self.jobs_table.c.job_data])
        if end_time:
            query = query.where(self.jobs_table.c.next_run_time <= end_time)

        results = self.engine.execute(query)
        jobs = []
        for id, job in results:
            job.id = id
            jobs.append(job)
        return jobs

    def get_next_run_time(self, start_time):
        query = select([func.min(self.jobs_table.c.next_run_time)]).\
            where(self.jobs_table.c.next_run_time > start_time)
        return self.engine.execute(query).scalar()

    def str(self):
        return '%s (%s, %s)' % (self.alias, self.__class__.__name__,
                                self.engine.url)
