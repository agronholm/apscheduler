"""
Example demonstrating use with the FastAPI web framework.

Requires the "postgresql" service to be running.
To install prerequisites: pip install sqlalchemy asycnpg fastapi uvicorn
To run: uvicorn asgi_fastapi:app

It should print a line on the console on a one-second interval while running a
basic web app at http://localhost:8000.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI
from fastapi.responses import PlainTextResponse, Response
from sqlalchemy.ext.asyncio import create_async_engine

from apscheduler import AsyncScheduler
from apscheduler.datastores.sqlalchemy import SQLAlchemyDataStore
from apscheduler.eventbrokers.asyncpg import AsyncpgEventBroker
from apscheduler.triggers.interval import IntervalTrigger


def tick():
    print("Hello, the time is", datetime.now())


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None]:
    engine = create_async_engine(
        "postgresql+asyncpg://postgres:secret@localhost/testdb"
    )
    data_store = SQLAlchemyDataStore(engine)
    event_broker = AsyncpgEventBroker.from_async_sqla_engine(engine)
    scheduler = AsyncScheduler(data_store, event_broker)

    async with scheduler:
        await scheduler.add_schedule(tick, IntervalTrigger(seconds=1), id="tick")
        await scheduler.start_in_background()
        yield


async def root() -> Response:
    return PlainTextResponse("Hello, world!")


app = FastAPI(lifespan=lifespan)
app.add_api_route("/", root)
