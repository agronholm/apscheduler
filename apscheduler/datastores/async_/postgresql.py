from __future__ import annotations

import json
import logging
from contextlib import AsyncExitStack
from datetime import datetime, timezone
from json import JSONDecodeError
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Type
from uuid import UUID

import sniffio
from anyio import TASK_STATUS_IGNORED, create_task_group, sleep
from anyio.abc import TaskGroup
from asyncpg import Connection, UniqueViolationError
from asyncpg.pool import Pool
from attr import asdict

from ... import events as events_module
from ...abc import AsyncDataStore, Job, Schedule, Serializer
from ...events import (
    AsyncEventHub, DataStoreEvent, Event, JobAdded, ScheduleAdded, ScheduleRemoved,
    ScheduleUpdated, SubscriptionToken)
from ...exceptions import ConflictingIdError, SerializationError
from ...policies import ConflictPolicy
from ...serializers.pickle import PickleSerializer
from ...util import reentrant


def default_json_handler(obj: Any) -> Any:
    if isinstance(obj, datetime):
        return obj.timestamp()


def json_object_hook(obj: Dict[str, Any]) -> Any:
    for key, value in obj:
        if key == 'timestamp':
            obj[key] = datetime.fromtimestamp(value, timezone.utc)

    return obj


@reentrant
class PostgresqlDataStore(AsyncDataStore):
    _task_group: TaskGroup

    def __init__(self, pool: Pool, *, schema: str = 'public',
                 notify_channel: Optional[str] = 'apscheduler',
                 serializer: Optional[Serializer] = None,
                 lock_expiration_delay: float = 30, max_idle_time: float = 60,
                 start_from_scratch: bool = False):
        self.pool = pool
        self.schema = schema
        self.notify_channel = notify_channel
        self.serializer = serializer or PickleSerializer()
        self.lock_expiration_delay = lock_expiration_delay
        self.max_idle_time = max_idle_time
        self.start_from_scratch = start_from_scratch
        self._logger = logging.getLogger(__name__)
        self._exit_stack = AsyncExitStack()
        self._events = AsyncEventHub()

    async def __aenter__(self):
        asynclib = sniffio.current_async_library() or '(unknown)'
        if asynclib != 'asyncio':
            raise RuntimeError(f'This data store requires asyncio; currently running: {asynclib}')

        await self._exit_stack.__aenter__()
        await self._exit_stack.enter_async_context(self._events)

        async with self.pool.acquire() as conn, conn.transaction():
            if self.start_from_scratch:
                await conn.execute(f"DROP TABLE IF EXISTS {self.schema}.schedules")
                await conn.execute(f"DROP TABLE IF EXISTS {self.schema}.jobs")
                await conn.execute(f"DROP TABLE IF EXISTS {self.schema}.metadata")

            await conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.schema}.metadata (
                    schema_version INTEGER NOT NULL
                )
            """)
            version = await conn.fetchval(f"SELECT schema_version FROM {self.schema}.metadata")
            if version is None:
                await conn.execute(f"INSERT INTO {self.schema}.metadata VALUES (1)")
                await conn.execute(f"""
                    CREATE TABLE {self.schema}.schedules (
                        id TEXT PRIMARY KEY,
                        task_id TEXT NOT NULL,
                        serialized_data BYTEA NOT NULL,
                        next_fire_time TIMESTAMP WITH TIME ZONE,
                        acquired_by TEXT,
                        acquired_until TIMESTAMP WITH TIME ZONE
                    ) WITH (fillfactor = 80)
                """)
                await conn.execute(f"CREATE INDEX ON {self.schema}.schedules (next_fire_time)")
                await conn.execute(f"""
                    CREATE TABLE {self.schema}.jobs (
                        id UUID PRIMARY KEY,
                        serialized_data BYTEA NOT NULL,
                        task_id TEXT NOT NULL,
                        tags TEXT[] NOT NULL,
                        created_at TIMESTAMP WITH TIME ZONE NOT NULL,
                        acquired_by TEXT,
                        acquired_until TIMESTAMP WITH TIME ZONE
                    ) WITH (fillfactor = 80)
                """)
                await conn.execute(f"CREATE INDEX ON {self.schema}.jobs (task_id)")
                await conn.execute(f"CREATE INDEX ON {self.schema}.jobs (tags)")
            elif version > 1:
                raise RuntimeError(f'Unexpected schema version ({version}); '
                                   f'only version 1 is supported by this version of APScheduler')

        if self.notify_channel:
            self._task_group = create_task_group()
            await self._task_group.__aenter__()
            await self._task_group.start(self._listen_notifications)

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.notify_channel:
            self._task_group.cancel_scope.cancel()
            await self._task_group.__aexit__(exc_type, exc_val, exc_tb)

    def subscribe(self, callback: Callable[[Event], Any],
                  event_types: Optional[Iterable[Type[Event]]] = None) -> SubscriptionToken:
        return self._events.subscribe(callback, event_types)

    def unsubscribe(self, token: SubscriptionToken) -> None:
        self._events.unsubscribe(token)

    async def _notify(self, conn: Connection, event: DataStoreEvent) -> None:
        if self.notify_channel:
            event_type = event.__class__.__name__
            event_data = json.dumps(asdict(event), ensure_ascii=False,
                                    default=default_json_handler)
            notification = event_type + ' ' + event_data
            if len(notification) < 8000:
                await conn.execute(f"NOTIFY {self.notify_channel}, {notification!r}")
                return

            self._logger.warning(
                'Could not send %s notification because it is too long (%d >= 8000)',
                event_type, len(notification))

        self._events.publish(event)

    async def _listen_notifications(self, *, task_status=TASK_STATUS_IGNORED) -> None:
        def callback(connection, pid, channel: str, payload: str) -> None:
            self._logger.debug('Received notification on channel %s: %s', channel, payload)
            event_type, _, json_data = payload.partition(' ')
            try:
                event_data = json.loads(json_data)
            except JSONDecodeError:
                self._logger.exception('Failed decoding JSON payload of notification: %s', payload)
                return

            event_class = getattr(events_module, event_type)
            event = event_class(**event_data)
            self._events.publish(event)

        task_started_sent = False
        while True:
            async with self.pool.acquire() as conn:
                await conn.add_listener(self.notify_channel, callback)
                if not task_started_sent:
                    task_status.started()
                    task_started_sent = True

                try:
                    while True:
                        await sleep(self.max_idle_time)
                        await conn.execute('SELECT 1')
                finally:
                    await conn.remove_listener(self.notify_channel, callback)

    async def clear(self) -> None:
        async with self.pool.acquire() as conn, conn.transaction():
            await conn.execute(f"TRUNCATE TABLE {self.schema}.schedules")
            await conn.execute(f"TRUNCATE TABLE {self.schema}.jobs")

    async def add_schedule(self, schedule: Schedule, conflict_policy: ConflictPolicy) -> None:
        serialized_data = self.serializer.serialize(schedule)
        query = (f"INSERT INTO {self.schema}.schedules (id, serialized_data, task_id, "
                 f"next_fire_time) VALUES ($1, $2, $3, $4)")
        async with self.pool.acquire() as conn:
            try:
                async with conn.transaction():
                    await conn.execute(query, schedule.id, serialized_data, schedule.task_id,
                                       schedule.next_fire_time)
            except UniqueViolationError:
                if conflict_policy is ConflictPolicy.exception:
                    raise ConflictingIdError(schedule.id) from None
                elif conflict_policy is ConflictPolicy.replace:
                    query = (f"UPDATE {self.schema}.schedules SET serialized_data = $2, "
                             f"task_id = $3, next_fire_time = $4 WHERE id = $1")
                    async with conn.transaction():
                        await conn.execute(query, schedule.id, serialized_data, schedule.task_id,
                                           schedule.next_fire_time)

                    event = ScheduleUpdated(
                        schedule_id=schedule.id, next_fire_time=schedule.next_fire_time)
            else:
                event = ScheduleAdded(
                    schedule_id=schedule.id, next_fire_time=schedule.next_fire_time)

            await self._notify(conn, event)

    async def remove_schedules(self, ids: Iterable[str]) -> None:
        async with self.pool.acquire() as conn, conn.transaction():
            query = (f"DELETE FROM {self.schema}.schedules "
                     f"WHERE id = any($1::text[]) "
                     f"  AND (acquired_until IS NULL OR acquired_until < $2) "
                     f"RETURNING id")
            now = datetime.now(timezone.utc)
            removed_ids = [row[0] for row in await conn.fetch(query, list(ids), now)]
            for schedule_id in removed_ids:
                event = ScheduleRemoved(schedule_id=schedule_id)
                self._events.publish(event)

    async def get_schedules(self, ids: Optional[Set[str]] = None) -> List[Schedule]:
        query = f"SELECT serialized_data FROM {self.schema}.schedules"
        args = ()
        if ids:
            query += " WHERE id = any($1::text[])"
            args = (ids,)

        query += " ORDER BY id"
        records = await self.pool.fetch(query, *args)
        return [self.serializer.deserialize(r[0]) for r in records]

    async def acquire_schedules(self, scheduler_id: str, limit: int) -> List[Schedule]:
        schedules: List[Schedule] = []
        async with self.pool.acquire() as conn, conn.transaction():
            acquired_until = datetime.fromtimestamp(
                datetime.now(timezone.utc).timestamp() + self.lock_expiration_delay,
                timezone.utc)
            records = await conn.fetch(f"""
                WITH schedule_ids AS (
                    SELECT id FROM {self.schema}.schedules
                    WHERE next_fire_time IS NOT NULL AND next_fire_time <= $1
                        AND (acquired_until IS NULL OR $1 > acquired_until)
                    ORDER BY next_fire_time
                    FOR NO KEY UPDATE SKIP LOCKED
                    FETCH FIRST $2 ROWS ONLY
                )
                UPDATE {self.schema}.schedules SET acquired_by = $3, acquired_until = $4
                WHERE id IN (SELECT id FROM schedule_ids)
                RETURNING serialized_data
                """, datetime.now(timezone.utc), limit, scheduler_id, acquired_until)

        for record in records:
            schedule = self.serializer.deserialize(record['serialized_data'])
            schedules.append(schedule)

        return schedules

    async def release_schedules(self, scheduler_id: str, schedules: List[Schedule]) -> None:
        events: List[DataStoreEvent] = []
        finished_schedule_ids: List[str] = []
        async with self.pool.acquire() as conn, conn.transaction():
            update_args = []
            for schedule in schedules:
                if schedule.next_fire_time is not None:
                    try:
                        serialized_data = self.serializer.serialize(schedule)
                    except SerializationError:
                        self._logger.exception('Error serializing schedule %r – '
                                               'removing from data store', schedule.id)
                        finished_schedule_ids.append(schedule.id)
                        continue

                    update_args.append((serialized_data, schedule.next_fire_time, schedule.id))
                    events.append(
                        ScheduleUpdated(schedule_id=schedule.id,
                                        next_fire_time=schedule.next_fire_time)
                    )
                else:
                    finished_schedule_ids.append(schedule.id)

            # Update schedules that have a next fire time
            if update_args:
                await conn.executemany(
                    f"UPDATE {self.schema}.schedules SET serialized_data = $1, "
                    f"next_fire_time = $2, acquired_by = NULL, acquired_until = NULL "
                    f"WHERE id = $3 AND acquired_by = {scheduler_id!r}", update_args)

            # Remove schedules that have no next fire time or failed to serialize
            if finished_schedule_ids:
                await conn.execute(
                    f"DELETE FROM {self.schema}.schedules "
                    f"WHERE id = any($1::text[]) AND acquired_by = $2",
                    list(finished_schedule_ids), scheduler_id)
                for schedule_id in finished_schedule_ids:
                    events.append(ScheduleRemoved(schedule_id=schedule_id))

            for event in events:
                await self._notify(conn, event)

    async def add_job(self, job: Job) -> None:
        now = datetime.now(timezone.utc)
        query = (f"INSERT INTO {self.schema}.jobs (id, task_id, created_at, serialized_data, "
                 f"tags) VALUES($1, $2, $3, $4, $5)")
        async with self.pool.acquire() as conn:
            await conn.execute(query, job.id, job.task_id, now, self.serializer.serialize(job),
                               job.tags)
            event = JobAdded(job_id=job.id, task_id=job.task_id, schedule_id=job.schedule_id,
                             tags=job.tags)

            await self._notify(conn, event)

    async def get_jobs(self, ids: Optional[Iterable[UUID]] = None) -> List[Job]:
        query = f"SELECT serialized_data FROM {self.schema}.jobs"
        args = ()
        if ids:
            query += " WHERE id = any($1::uuid[])"
            args = (ids,)

        query += " ORDER BY id"
        records = await self.pool.fetch(query, *args)
        return [self.serializer.deserialize(r[0]) for r in records]

    async def acquire_jobs(self, worker_id: str, limit: Optional[int] = None) -> List[Job]:
        jobs: List[Job] = []
        async with self.pool.acquire() as conn, conn.transaction():
            now = datetime.now(timezone.utc)
            acquired_until = datetime.fromtimestamp(
                now.timestamp() + self.lock_expiration_delay, timezone.utc)
            records = await conn.fetch(f"""
                WITH job_ids AS (
                    SELECT id FROM {self.schema}.jobs
                    WHERE acquired_until IS NULL OR acquired_until < $1
                    ORDER BY created_at
                    FOR NO KEY UPDATE SKIP LOCKED
                    FETCH FIRST $2 ROWS ONLY
                )
                UPDATE {self.schema}.jobs SET acquired_by = $3, acquired_until = $4
                WHERE id IN (SELECT id FROM job_ids)
                RETURNING serialized_data
                """, now, limit, worker_id, acquired_until)

        for record in records:
            job = self.serializer.deserialize(record['serialized_data'])
            jobs.append(job)

        return jobs

    async def release_jobs(self, worker_id: str, jobs: List[Job]) -> None:
        job_ids = {j.id for j in jobs}
        await self.pool.execute(
            f"DELETE FROM {self.schema}.jobs WHERE acquired_by = $1 AND id = any($2::uuid[])",
            worker_id, job_ids)
