"""
Stores jobs in a database table using SQLAlchemy.
"""

from datetime import datetime
from copy import copy

from apscheduler.jobstores.base import JobStore
from apscheduler.job import JobMeta

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
    stores_persistent = True

    def __init__(self, engine=None, url=None, tablename=None):
        if engine:
            self.url = engine.url
        elif url:
            engine = create_engine(url)
            self.url = url
        else:
            raise ValueError('Need either "engine" or "url" defined')

        self.sessionmaker = sessionmaker(bind=engine)
        if tablename:
            jobmeta_table.name = tablename
        jobmeta_table.create(engine, True)

    def _export_jobmeta(self, jobmeta):
        jobmeta = copy(jobmeta)
        make_transient(jobmeta)
        jobmeta.jobstore = self
        return jobmeta

    @session
    def add_job(self, session, jobmeta):
        session.add(jobmeta)
        jobmeta.jobstore = self

    @session
    def remove_job(self, session, jobmeta):
        session.query(JobMeta).filter_by(id=jobmeta.id).delete(False)

    @session
    def checkout_jobs(self, session, end_time):
        query = session.query(JobMeta).\
            filter(JobMeta.next_run_time <= end_time).\
            filter(JobMeta.checkout_time == None).\
            with_lockmode('update')
        now = datetime.now()
        jobmetas = []
        for jobmeta in query:
            jobmetas.append(self._export_jobmeta(jobmeta))

            # Mark this job as started and compute the next run time
            jobmeta.checkout_time = now
            jobmeta.next_run_time = jobmeta.trigger.get_next_fire_time(now)
        return jobmetas

    @session
    def checkin_job(self, session, jobmeta):
        storedmeta = session.query(JobMeta).get(jobmeta.id)
        storedmeta.job = jobmeta.job
        storedmeta.checkout_time = None

    @session
    def list_jobs(self, session):
        query = session.query(JobMeta)
        jobmetas = []
        for jobmeta in query:
            jobmetas.append(self._export_jobmeta(jobmeta))
        return jobmetas

    @session
    def get_next_run_time(self, session, start_time):
        return session.query(func.min(JobMeta.next_run_time)).scalar()

    def __repr__(self):
        return '%s (%s)' % (self.__class__.__name__, self.url)


jobmeta_table = Table('apscheduler_jobs', MetaData(),
    Column('id', Integer, primary_key=True),
    Column('job', PickleType(mutable=False), nullable=False),
    Column('trigger', PickleType(mutable=False), nullable=False),
    Column('name', Unicode(1024), unique=True),
    Column('next_run_time', DateTime, nullable=False, index=True),
    Column('misfire_grace_time', Integer, nullable=False),
    Column('checkout_time', DateTime))

mapper(JobMeta, jobmeta_table)
