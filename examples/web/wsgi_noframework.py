"""
Example demonstrating use with WSGI (raw WSGI application, no framework).

Requires the "postgresql" service to be running.
To install prerequisites: pip install sqlalchemy psycopg2 uwsgi
To run: uwsgi -T --http :8000 --wsgi-file wsgi_noframework.py

It should print a line on the console on a one-second interval while running a
basic web app at http://localhost:8000.
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy.future import create_engine

from apscheduler.datastores.sqlalchemy import SQLAlchemyDataStore
from apscheduler.schedulers.sync import Scheduler
from apscheduler.triggers.interval import IntervalTrigger


def tick():
    print("Hello, the time is", datetime.now())


def application(environ, start_response):
    response_body = b"Hello, World!"
    response_headers = [
        ("Content-Type", "text/plain"),
        ("Content-Length", str(len(response_body))),
    ]
    start_response("200 OK", response_headers)
    return [response_body]


engine = create_engine("postgresql+psycopg2://postgres:secret@localhost/testdb")
data_store = SQLAlchemyDataStore(engine)
scheduler = Scheduler(data_store)
scheduler.add_schedule(tick, IntervalTrigger(seconds=1), id="tick")
scheduler.start_in_background()
