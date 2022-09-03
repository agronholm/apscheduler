"""
Example demonstrating use with ASGI (raw ASGI application, no framework).

Requires the "postgresql" service to be running.
To install prerequisites: pip install sqlalchemy asyncpg uvicorn
To run: uvicorn asgi_noframework:app

It should print a line on the console on a one-second interval while running a
basic web app at http://localhost:8000.
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy.ext.asyncio import create_async_engine

from apscheduler.datastores.async_sqlalchemy import AsyncSQLAlchemyDataStore
from apscheduler.eventbrokers.asyncpg import AsyncpgEventBroker
from apscheduler.schedulers.async_ import AsyncScheduler
from apscheduler.triggers.interval import IntervalTrigger


def tick():
    print("Hello, the time is", datetime.now())


async def original_app(scope, receive, send):
    """Trivial example of an ASGI application."""
    if scope["type"] == "http":
        await receive()
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [
                    [b"content-type", b"text/plain"],
                ],
            }
        )
        await send(
            {
                "type": "http.response.body",
                "body": b"Hello, world!",
                "more_body": False,
            }
        )
    elif scope["type"] == "lifespan":
        while True:
            message = await receive()
            if message["type"] == "lifespan.startup":
                await send({"type": "lifespan.startup.complete"})
            elif message["type"] == "lifespan.shutdown":
                await send({"type": "lifespan.shutdown.complete"})
                return


async def scheduler_middleware(scope, receive, send):
    if scope["type"] == "lifespan":
        engine = create_async_engine(
            "postgresql+asyncpg://postgres:secret@localhost/testdb"
        )
        data_store = AsyncSQLAlchemyDataStore(engine)
        event_broker = AsyncpgEventBroker.from_async_sqla_engine(engine)
        async with AsyncScheduler(data_store, event_broker) as scheduler:
            await scheduler.add_schedule(tick, IntervalTrigger(seconds=1), id="tick")
            await scheduler.start_in_background()
            await original_app(scope, receive, send)
    else:
        await original_app(scope, receive, send)


# This is just for consistency with the other ASGI examples
app = scheduler_middleware
