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

import logging

from example_tasks import tick
from sqlalchemy.future import create_engine

from apscheduler.datastores.sqlalchemy import SQLAlchemyDataStore
from apscheduler.eventbrokers.redis import RedisEventBroker
from apscheduler.schedulers.sync import Scheduler
from apscheduler.triggers.interval import IntervalTrigger

logging.basicConfig(level=logging.INFO)
engine = create_engine("postgresql+psycopg2://postgres:secret@localhost/testdb")
data_store = SQLAlchemyDataStore(engine)
event_broker = RedisEventBroker.from_url("redis://localhost")

# Uncomment the next two lines to use the MQTT event broker instead
# from apscheduler.eventbrokers.mqtt import MQTTEventBroker
# event_broker = MQTTEventBroker()

with Scheduler(data_store, event_broker, start_worker=False) as scheduler:
    scheduler.add_schedule(tick, IntervalTrigger(seconds=1), id="tick")
    scheduler.run_until_stopped()
