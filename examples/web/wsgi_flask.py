"""
Example demonstrating use with WSGI (raw WSGI application, no framework).

Requires the "postgresql" service to be running.
To install prerequisites: pip install sqlalchemy psycopg2 flask uwsgi
To run: uwsgi -T --http :8000 --wsgi-file wsgi_flask.py

It should print a line on the console on a one-second interval while running a
basic web app at http://localhost:8000.
"""

from __future__ import annotations

from datetime import datetime

from flask import Flask
from sqlalchemy.future import create_engine

from apscheduler.datastores.sqlalchemy import SQLAlchemyDataStore
from apscheduler.eventbrokers.redis import RedisEventBroker
from apscheduler.schedulers.sync import Scheduler
from apscheduler.triggers.interval import IntervalTrigger

app = Flask(__name__)


def tick():
    print("Hello, the time is", datetime.now())


@app.route("/")
def hello_world():
    return "<p>Hello, World!</p>"


engine = create_engine("postgresql+psycopg2://postgres:secret@localhost/testdb")
data_store = SQLAlchemyDataStore(engine)
event_broker = RedisEventBroker.from_url("redis://localhost")
scheduler = Scheduler(data_store, event_broker)
scheduler.add_schedule(tick, IntervalTrigger(seconds=1), id="tick")
scheduler.start_in_background()
