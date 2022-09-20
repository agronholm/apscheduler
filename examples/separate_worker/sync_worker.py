"""
Example demonstrating a scheduler that only runs jobs but does not process schedules.
You need to be running both this and the scheduler script simultaneously in order for
the scheduled task to be run.

Requires the "postgresql" and "redis" services to be running.
To install prerequisites: pip install sqlalchemy psycopg2 redis
To run: python sync_worker.py

When run together with sync_scheduler.py, it should print a line on the
console on a one-second interval.
"""

from __future__ import annotations

import logging

from sqlalchemy.future import create_engine

from apscheduler import SchedulerRole
from apscheduler.datastores.sqlalchemy import SQLAlchemyDataStore
from apscheduler.eventbrokers.redis import RedisEventBroker
from apscheduler.schedulers.sync import Scheduler

logging.basicConfig(level=logging.INFO)
engine = create_engine("postgresql+psycopg2://postgres:secret@localhost/testdb")
data_store = SQLAlchemyDataStore(engine)
event_broker = RedisEventBroker.from_url("redis://localhost")

# Uncomment the next two lines to use the MQTT event broker instead
# from apscheduler.eventbrokers.mqtt import MQTTEventBroker
# event_broker = MQTTEventBroker()

with Scheduler(data_store, event_broker, role=SchedulerRole.worker) as scheduler:
    scheduler.run_until_stopped()
