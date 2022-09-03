"""
Example demonstrating use with the Starlette web framework.

Requires the "postgresql" service to be running.
To install prerequisites: pip install sqlalchemy asycnpg starlette uvicorn
To run: uvicorn asgi_starlette:app

It should print a line on the console on a one-second interval while running a
basic web app at http://localhost:8000.
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy.ext.asyncio import create_async_engine
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.requests import Request
from starlette.responses import PlainTextResponse, Response
from starlette.routing import Route
from starlette.types import ASGIApp, Receive, Scope, Send

from apscheduler.datastores.async_sqlalchemy import AsyncSQLAlchemyDataStore
from apscheduler.eventbrokers.asyncpg import AsyncpgEventBroker
from apscheduler.schedulers.async_ import AsyncScheduler
from apscheduler.triggers.interval import IntervalTrigger


def tick():
    print("Hello, the time is", datetime.now())


class SchedulerMiddleware:
    def __init__(
        self,
        app: ASGIApp,
        scheduler: AsyncScheduler,
    ) -> None:
        self.app = app
        self.scheduler = scheduler

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "lifespan":
            async with self.scheduler:
                await self.scheduler.add_schedule(
                    tick, IntervalTrigger(seconds=1), id="tick"
                )
                await self.scheduler.start_in_background()
                await self.app(scope, receive, send)
        else:
            await self.app(scope, receive, send)


async def root(request: Request) -> Response:
    return PlainTextResponse("Hello, world!")


engine = create_async_engine("postgresql+asyncpg://postgres:secret@localhost/testdb")
data_store = AsyncSQLAlchemyDataStore(engine)
event_broker = AsyncpgEventBroker.from_async_sqla_engine(engine)
scheduler = AsyncScheduler()
routes = [Route("/", root)]
middleware = [Middleware(SchedulerMiddleware, scheduler=scheduler)]
app = Starlette(routes=routes, middleware=middleware)
