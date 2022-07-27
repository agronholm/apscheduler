"""
Example demonstrating the separation of scheduler and worker.
This script runs the scheduler part. You need to be running both this and the worker
script simultaneously in order for the scheduled task to be run.

Requires the "postgresql" and "redis" services to be running.
To install prerequisites: pip install sqlalchemy psycopg2 redis
To run: python sync_scheduler.py

When run together with sync_worker.py, it should print a line on the console
on a one-second interval.
"""

from __future__ import annotations

from example_tasks import tick
from sqlalchemy.future import create_engine

from apscheduler.datastores.sqlalchemy import SQLAlchemyDataStore
from apscheduler.eventbrokers.redis import RedisEventBroker
from apscheduler.schedulers.sync import Scheduler
from apscheduler.triggers.interval import IntervalTrigger

engine = create_engine("postgresql+psycopg2://postgres:secret@localhost/testdb")
data_store = SQLAlchemyDataStore(engine)
event_broker = RedisEventBroker.from_url("redis://localhost")
with Scheduler(data_store, event_broker, start_worker=False) as scheduler:
    scheduler.add_schedule(tick, IntervalTrigger(seconds=1), id="tick")
    scheduler.wait_until_stopped()
