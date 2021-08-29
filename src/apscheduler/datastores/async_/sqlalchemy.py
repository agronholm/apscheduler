from __future__ import annotations

import json
import logging
from contextlib import AsyncExitStack, asynccontextmanager, closing
from datetime import datetime, timedelta, timezone
from json import JSONDecodeError
from typing import (
    Any, AsyncGenerator, Callable, Dict, Iterable, List, Optional, Set, Tuple, Type, Union)
from uuid import UUID

import sniffio
from anyio import TASK_STATUS_IGNORED, create_task_group, sleep
from attr import asdict
from sqlalchemy import (
    Column, DateTime, Integer, LargeBinary, MetaData, Table, Unicode, and_, bindparam, func, or_,
    select)
from sqlalchemy.engine import URL
from sqlalchemy.exc import CompileError, IntegrityError
from sqlalchemy.ext.asyncio import AsyncConnection, create_async_engine
from sqlalchemy.ext.asyncio.engine import AsyncConnectable
from sqlalchemy.sql.ddl import DropTable

from ... import events as events_module
from ...abc import AsyncDataStore, Job, Schedule, Serializer
from ...events import (
    AsyncEventHub, DataStoreEvent, Event, JobAdded, JobDeserializationFailed, ScheduleAdded,
    ScheduleDeserializationFailed, ScheduleRemoved, ScheduleUpdated, SubscriptionToken)
from ...exceptions import ConflictingIdError, SerializationError
from ...policies import ConflictPolicy
from ...serializers.pickle import PickleSerializer
from ...util import reentrant

logger = logging.getLogger(__name__)


def default_json_handler(obj: Any) -> Any:
    if isinstance(obj, datetime):
        return obj.timestamp()
    elif isinstance(obj, UUID):
        return obj.hex
    elif isinstance(obj, frozenset):
        return list(obj)

    raise TypeError(f'Cannot JSON encode type {type(obj)}')


def json_object_hook(obj: Dict[str, Any]) -> Any:
    for key, value in obj.items():
        if key == 'timestamp':
            obj[key] = datetime.fromtimestamp(value, timezone.utc)
        elif key == 'job_id':
            obj[key] = UUID(value)
        elif key == 'tags':
            obj[key] = frozenset(value)

    return obj


@reentrant
class SQLAlchemyDataStore(AsyncDataStore):
    _metadata = MetaData()

    t_metadata = Table(
        'metadata',
        _metadata,
        Column('schema_version', Integer, nullable=False)
    )
    t_schedules = Table(
        'schedules',
        _metadata,
        Column('id', Unicode, primary_key=True),
        Column('task_id', Unicode, nullable=False),
        Column('serialized_data', LargeBinary, nullable=False),
        Column('next_fire_time', DateTime(timezone=True), index=True)
    )
    t_jobs = Table(
        'jobs',
        _metadata,
        Column('id', Unicode(32), primary_key=True),
        Column('task_id', Unicode, nullable=False, index=True),
        Column('serialized_data', LargeBinary, nullable=False),
        Column('created_at', DateTime(timezone=True), nullable=False),
        Column('acquired_by', Unicode),
        Column('acquired_until', DateTime(timezone=True))
    )

    def __init__(self, bind: AsyncConnectable, *, schema: Optional[str] = None,
                 serializer: Optional[Serializer] = None,
                 lock_expiration_delay: float = 30, max_poll_time: Optional[float] = 1,
                 max_idle_time: float = 60, start_from_scratch: bool = False,
                 notify_channel: Optional[str] = 'apscheduler'):
        self.bind = bind
        self.schema = schema
        self.serializer = serializer or PickleSerializer()
        self.lock_expiration_delay = lock_expiration_delay
        self.max_poll_time = max_poll_time
        self.max_idle_time = max_idle_time
        self.start_from_scratch = start_from_scratch
        self._logger = logging.getLogger(__name__)
        self._exit_stack = AsyncExitStack()
        self._events = AsyncEventHub()

        # Find out if the dialect supports RETURNING
        statement = self.t_jobs.update().returning(self.t_schedules.c.id)
        try:
            statement.compile(bind=self.bind)
        except CompileError:
            self._supports_update_returning = False
        else:
            self._supports_update_returning = True

        self.notify_channel = notify_channel
        if notify_channel:
            if self.bind.dialect.name != 'postgresql' or self.bind.dialect.driver != 'asyncpg':
                self.notify_channel = None

    @classmethod
    def from_url(cls, url: Union[str, URL], **options) -> 'SQLAlchemyDataStore':
        engine = create_async_engine(url, future=True)
        return cls(engine, **options)

    async def __aenter__(self):
        asynclib = sniffio.current_async_library() or '(unknown)'
        if asynclib != 'asyncio':
            raise RuntimeError(f'This data store requires asyncio; currently running: {asynclib}')

        # Verify that the schema is in place
        async with self.bind.begin() as conn:
            if self.start_from_scratch:
                for table in self._metadata.sorted_tables:
                    await conn.execute(DropTable(table, if_exists=True))

            await conn.run_sync(self._metadata.create_all)
            query = select(self.t_metadata.c.schema_version)
            result = await conn.execute(query)
            version = result.scalar()
            if version is None:
                await conn.execute(self.t_metadata.insert(values={'schema_version': 1}))
            elif version > 1:
                raise RuntimeError(f'Unexpected schema version ({version}); '
                                   f'only version 1 is supported by this version of APScheduler')

        await self._exit_stack.enter_async_context(self._events)

        if self.notify_channel:
            task_group = create_task_group()
            await self._exit_stack.enter_async_context(task_group)
            await task_group.start(self._listen_notifications)
            self._exit_stack.callback(task_group.cancel_scope.cancel)

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._exit_stack.__aexit__(exc_type, exc_val, exc_tb)

    async def _publish(self, conn: AsyncConnection, event: DataStoreEvent) -> None:
        if self.notify_channel:
            event_type = event.__class__.__name__
            event_data = json.dumps(asdict(event), ensure_ascii=False,
                                    default=default_json_handler)
            notification = event_type + ' ' + event_data
            if len(notification) < 8000:
                await conn.execute(func.pg_notify(self.notify_channel, notification))
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
                event_data = json.loads(json_data, object_hook=json_object_hook)
            except JSONDecodeError:
                self._logger.exception('Failed decoding JSON payload of notification: %s', payload)
                return

            event_class = getattr(events_module, event_type)
            event = event_class(**event_data)
            self._events.publish(event)

        task_started_sent = False
        while True:
            with closing(await self.bind.raw_connection()) as conn:
                asyncpg_conn = conn.connection._connection
                await asyncpg_conn.add_listener(self.notify_channel, callback)
                if not task_started_sent:
                    task_status.started()
                    task_started_sent = True

                try:
                    while True:
                        await sleep(self.max_idle_time)
                        await asyncpg_conn.execute('SELECT 1')
                finally:
                    await asyncpg_conn.remove_listener(self.notify_channel, callback)

    def _deserialize_jobs(self, serialized_jobs: Iterable[Tuple[UUID, bytes]]) -> List[Job]:
        jobs: List[Job] = []
        for job_id, serialized_data in serialized_jobs:
            try:
                jobs.append(self.serializer.deserialize(serialized_data))
            except SerializationError as exc:
                self._events.publish(JobDeserializationFailed(job_id=job_id, exception=exc))

        return jobs

    def _deserialize_schedules(
            self, serialized_schedules: Iterable[Tuple[str, bytes]]) -> List[Schedule]:
        jobs: List[Schedule] = []
        for schedule_id, serialized_data in serialized_schedules:
            try:
                jobs.append(self.serializer.deserialize(serialized_data))
            except SerializationError as exc:
                self._events.publish(
                    ScheduleDeserializationFailed(schedule_id=schedule_id, exception=exc))

        return jobs

    def subscribe(self, callback: Callable[[Event], Any],
                  event_types: Optional[Iterable[Type[Event]]] = None) -> SubscriptionToken:
        return self._events.subscribe(callback, event_types)

    def unsubscribe(self, token: SubscriptionToken) -> None:
        self._events.unsubscribe(token)

    async def clear(self) -> None:
        async with self.bind.begin() as conn:
            await conn.execute(self.t_schedules.delete())
            await conn.execute(self.t_jobs.delete())

    async def add_schedule(self, schedule: Schedule, conflict_policy: ConflictPolicy) -> None:
        serialized_data = self.serializer.serialize(schedule)
        statement = self.t_schedules.insert().\
            values(id=schedule.id, task_id=schedule.task_id, serialized_data=serialized_data,
                   next_fire_time=schedule.next_fire_time)
        try:
            async with self.bind.begin() as conn:
                await conn.execute(statement)
                event = ScheduleAdded(schedule_id=schedule.id,
                                      next_fire_time=schedule.next_fire_time)
                await self._publish(conn, event)
        except IntegrityError:
            if conflict_policy is ConflictPolicy.exception:
                raise ConflictingIdError(schedule.id) from None
            elif conflict_policy is ConflictPolicy.replace:
                statement = self.t_schedules.update().\
                    where(self.t_schedules.c.id == schedule.id).\
                    values(serialized_data=serialized_data,
                           next_fire_time=schedule.next_fire_time)
                async with self.bind.begin() as conn:
                    await conn.execute(statement)

                    event = ScheduleUpdated(schedule_id=schedule.id,
                                            next_fire_time=schedule.next_fire_time)
                    await self._publish(conn, event)

    async def remove_schedules(self, ids: Iterable[str]) -> None:
        async with self.bind.begin() as conn:
            statement = self.t_schedules.delete().where(self.t_schedules.c.id.in_(ids))
            if self._supports_update_returning:
                statement = statement.returning(self.t_schedules.c.id)
                removed_ids = [row[0] for row in await conn.execute(statement)]
            else:
                await conn.execute(statement)

            for schedule_id in removed_ids:
                await self._publish(conn, ScheduleRemoved(schedule_id=schedule_id))

    async def get_schedules(self, ids: Optional[Set[str]] = None) -> List[Schedule]:
        query = select([self.t_schedules.c.id, self.t_schedules.c.serialized_data]).\
            order_by(self.t_schedules.c.id)
        if ids:
            query = query.where(self.t_schedules.c.id.in_(ids))

        async with self.bind.begin() as conn:
            result = await conn.execute(query)

        return self._deserialize_schedules(result)

    @asynccontextmanager
    async def acquire_schedules(self, scheduler_id: str,
                                limit: int) -> AsyncGenerator[List[Schedule], None]:
        async with self.bind.begin() as conn:
            now = datetime.now(timezone.utc)
            statement = select([self.t_schedules.c.id, self.t_schedules.c.serialized_data]).\
                where(and_(self.t_schedules.c.next_fire_time.isnot(None),
                           self.t_schedules.c.next_fire_time <= now)).\
                with_for_update(skip_locked=True).limit(limit)
            result = await conn.execute(statement)
            schedules = self._deserialize_schedules(result)

            yield schedules

            update_events: List[ScheduleUpdated] = []
            finished_schedule_ids: List[str] = []
            update_args: List[Dict[str, Any]] = []
            for schedule in schedules:
                if schedule.next_fire_time is not None:
                    try:
                        serialized_data = self.serializer.serialize(schedule)
                    except SerializationError:
                        self._logger.exception('Error serializing schedule %r – '
                                               'removing from data store', schedule.id)
                        finished_schedule_ids.append(schedule.id)
                        continue

                    update_args.append({
                        'p_id': schedule.id,
                        'p_serialized_data': serialized_data,
                        'p_next_fire_time': schedule.next_fire_time
                    })
                else:
                    finished_schedule_ids.append(schedule.id)

            # Update schedules that have a next fire time
            if update_args:
                p_id = bindparam('p_id')
                p_serialized = bindparam('p_serialized_data')
                p_next_fire_time = bindparam('p_next_fire_time')
                statement = self.t_schedules.update().\
                    where(self.t_schedules.c.id == p_id).\
                    values(serialized_data=p_serialized, next_fire_time=p_next_fire_time)
                next_fire_times = {arg['p_id']: arg['p_next_fire_time'] for arg in update_args}
                if self._supports_update_returning:
                    statement = statement.returning(self.t_schedules.c.id)
                    updated_ids = [row[0] for row in await conn.execute(statement, update_args)]
                    for schedule_id in updated_ids:
                        event = ScheduleUpdated(schedule_id=schedule_id,
                                                next_fire_time=next_fire_times[schedule_id])
                        update_events.append(event)

            # Remove schedules that have no next fire time or failed to serialize
            if finished_schedule_ids:
                statement = self.t_schedules.delete().\
                    where(self.t_schedules.c.id.in_(finished_schedule_ids))
                await conn.execute(statement)

            for event in update_events:
                await self._publish(conn, event)

            for schedule_id in finished_schedule_ids:
                await self._publish(conn, ScheduleRemoved(schedule_id=schedule_id))

    async def add_job(self, job: Job) -> None:
        now = datetime.now(timezone.utc)
        serialized_data = self.serializer.serialize(job)
        statement = self.t_jobs.insert().values(id=job.id.hex, task_id=job.task_id,
                                                created_at=now, serialized_data=serialized_data)
        async with self.bind.begin() as conn:
            await conn.execute(statement)

            event = JobAdded(job_id=job.id, task_id=job.task_id, schedule_id=job.schedule_id,
                             tags=job.tags)
            await self._publish(conn, event)

    async def get_jobs(self, ids: Optional[Iterable[UUID]] = None) -> List[Job]:
        query = select([self.t_jobs.c.id, self.t_jobs.c.serialized_data]).\
            order_by(self.t_jobs.c.id)
        if ids:
            job_ids = [job_id.hex for job_id in ids]
            query = query.where(self.t_jobs.c.id.in_(job_ids))

        async with self.bind.begin() as conn:
            result = await conn.execute(query)

        return self._deserialize_jobs(result)

    async def acquire_jobs(self, worker_id: str, limit: Optional[int] = None) -> List[Job]:
        async with self.bind.begin() as conn:
            now = datetime.now(timezone.utc)
            acquired_until = now + timedelta(seconds=self.lock_expiration_delay)
            query = select([self.t_jobs.c.id, self.t_jobs.c.serialized_data]).\
                where(or_(self.t_jobs.c.acquired_until.is_(None),
                          self.t_jobs.c.acquired_until < now)).\
                order_by(self.t_jobs.c.created_at).\
                limit(limit)

            serialized_jobs: Dict[str, bytes] = {row[0]: row[1]
                                                 for row in await conn.execute(query)}
            if serialized_jobs:
                query = self.t_jobs.update().\
                    values(acquired_by=worker_id, acquired_until=acquired_until).\
                    where(self.t_jobs.c.id.in_(serialized_jobs))
                await conn.execute(query)

        return self._deserialize_jobs(serialized_jobs.items())

    async def release_jobs(self, worker_id: str, jobs: List[Job]) -> None:
        job_ids = [job.id.hex for job in jobs]
        statement = self.t_jobs.delete().\
            where(and_(self.t_jobs.c.acquired_by == worker_id, self.t_jobs.c.id.in_(job_ids)))

        async with self.bind.begin() as conn:
            await conn.execute(statement)
