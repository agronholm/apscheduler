"""
This is an example demonstrating how to run a scheduler to process schedules added by
another scheduler elsewhere. Prior to starting this script, you need to run the script
(either async_scheduler.py or sync_scheduler.py) that adds or updates a schedule to the
data store. This script will then pick up that schedule and start spawning jobs that
will print a line on the console on one-second intervals.

This script requires the "postgresql" service to be running.
To install prerequisites: pip install sqlalchemy asyncpg
To run: python sync_worker.py
"""

from __future__ import annotations

import logging

from sqlalchemy.ext.asyncio import create_async_engine

from apscheduler import Scheduler
from apscheduler.datastores.sqlalchemy import SQLAlchemyDataStore
from apscheduler.eventbrokers.asyncpg import AsyncpgEventBroker

logging.basicConfig(level=logging.INFO)
engine = create_async_engine("postgresql+asyncpg://postgres:secret@localhost/testdb")
data_store = SQLAlchemyDataStore(engine)
event_broker = AsyncpgEventBroker.from_async_sqla_engine(engine)

# Uncomment the next two lines to use the MQTT event broker instead
# from apscheduler.eventbrokers.mqtt import MQTTEventBroker
# event_broker = MQTTEventBroker()

with Scheduler(data_store, event_broker) as scheduler:
    scheduler.run_until_stopped()
