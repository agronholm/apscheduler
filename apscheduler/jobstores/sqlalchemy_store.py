"""
Stores jobs in a database table using SQLAlchemy.
"""

from apscheduler.jobstores.base import JobStore
from apscheduler.job import Job
from apscheduler.util import obj_to_ref

try:
    from sqlalchemy import *
    from sqlalchemy.orm import *
    from sqlalchemy.orm.session import sessionmaker
except ImportError:
    raise ImportError('SQLAlchemyJobStore requires SQLAlchemy installed')


def session(func):
    def wrapper(self, *args, **kwargs):
        session = self.sessionmaker()
        try:
            retval = func(self, session, *args, **kwargs)
        except:
            session.rollback()
            raise

        session.commit()
        return retval
    return wrapper


class SQLAlchemyJobStore(JobStore):
    def __init__(self, url=None, engine=None, tablename=None):
        self.jobs = []

        if engine:
            self.url = engine.url
        elif url:
            engine = create_engine(url)
            self.url = url
        else:
            raise ValueError('Need either "engine" or "url" defined')

        self.sessionmaker = sessionmaker(bind=engine)
        if tablename:
            jobs_table.name = tablename
        jobs_table.create(engine, True)

    def _export_job(self, job):
        make_transient(job)
        return job

    @session
    def add_job(self, session, job):
        job.func_ref = obj_to_ref(job.func)
        session.add(job)
        self.jobs.append(job)

    @session
    def remove_job(self, session, job):
        session.query(Job).filter_by(id=job.id).delete(False)
        self.jobs.remove(job)

    @session
    def load_jobs(self, session):
        self.jobs = [self._export_job(job) for job in session.query(Job)]

    @session
    def update_job(self, session, job):
        session.merge(job)

    def __repr__(self):
        return '<%s (url=%s)>' % (self.__class__.__name__, self.url)


jobs_table = Table('apscheduler_jobs', MetaData(),
    Column('id', Integer, primary_key=True),
    Column('trigger', PickleType(mutable=False), nullable=False),
    Column('func_ref', String(1024), nullable=False),
    Column('args', PickleType(mutable=False), nullable=False),
    Column('kwargs', PickleType(mutable=False), nullable=False),
    Column('name', Unicode(1024), unique=True),
    Column('misfire_grace_time', Integer, nullable=False),
    Column('max_runs', Integer),
    Column('max_concurrency', Integer),
    Column('next_run_time', DateTime, nullable=False),
    Column('runs', BigInteger))

mapper(Job, jobs_table)
