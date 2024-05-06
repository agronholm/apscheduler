from __future__ import annotations

import sys
from collections import defaultdict
from collections.abc import AsyncGenerator, Mapping, Sequence
from contextlib import AsyncExitStack, asynccontextmanager
from datetime import datetime, timedelta, timezone
from functools import partial
from logging import Logger
from typing import Any, Iterable
from uuid import UUID

import anyio
import attrs
import sniffio
import tenacity
from anyio import CancelScope, to_thread
from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    DateTime,
    Enum,
    Integer,
    Interval,
    LargeBinary,
    MetaData,
    SmallInteger,
    Table,
    TypeDecorator,
    Unicode,
    Uuid,
    and_,
    bindparam,
    false,
    or_,
    select,
)
from sqlalchemy.engine import URL, Dialect, Result
from sqlalchemy.exc import (
    CompileError,
    IntegrityError,
    InterfaceError,
    ProgrammingError,
)
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine
from sqlalchemy.future import Connection, Engine
from sqlalchemy.sql import Executable
from sqlalchemy.sql.ddl import DropTable
from sqlalchemy.sql.elements import BindParameter, literal
from sqlalchemy.sql.type_api import TypeEngine

from .._enums import CoalescePolicy, ConflictPolicy, JobOutcome
from .._events import (
    DataStoreEvent,
    JobAcquired,
    JobAdded,
    JobDeserializationFailed,
    JobReleased,
    ScheduleAdded,
    ScheduleDeserializationFailed,
    ScheduleRemoved,
    ScheduleUpdated,
    TaskAdded,
    TaskRemoved,
    TaskUpdated,
)
from .._exceptions import ConflictingIdError, SerializationError, TaskLookupError
from .._structures import Job, JobResult, Schedule, Task
from ..abc import EventBroker
from .base import BaseExternalDataStore

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


class EmulatedTimestampTZ(TypeDecorator[datetime]):
    impl = Unicode(32)
    cache_ok = True

    def process_bind_param(
        self, value: datetime | None, dialect: Dialect
    ) -> str | None:
        return value.isoformat() if value is not None else None

    def process_result_value(
        self, value: str | None, dialect: Dialect
    ) -> datetime | None:
        return datetime.fromisoformat(value) if value is not None else None


class EmulatedInterval(TypeDecorator[timedelta]):
    impl = BigInteger()
    cache_ok = True

    def process_bind_param(
        self, value: timedelta | None, dialect: Dialect
    ) -> str | None:
        return value.total_seconds() * 1000000 if value is not None else None

    def process_result_value(
        self, value: int | None, dialect: Dialect
    ) -> timedelta | None:
        return timedelta(seconds=value / 1000000) if value is not None else None


@attrs.define(eq=False)
class SQLAlchemyDataStore(BaseExternalDataStore):
    """
    Uses a relational database to store data.

    When started, this data store creates the appropriate tables on the given database
    if they're not already present.

    Operations are retried (in accordance to ``retry_settings``) when an operation
    raises :exc:`sqlalchemy.OperationalError`.

    This store has been tested to work with:

     * PostgreSQL (asyncpg and psycopg drivers)
     * MySQL (asyncmy driver)
     * aiosqlite

    :param engine: an asynchronous SQLAlchemy engine
    :param schema: a database schema name to use, if not the default
    """

    engine: Engine | AsyncEngine
    schema: str | None = attrs.field(default=None)
    max_poll_time: float | None = attrs.field(default=1)
    max_idle_time: float = attrs.field(default=60)

    _supports_update_returning: bool = attrs.field(init=False, default=False)
    _supports_tzaware_timestamps: bool = attrs.field(init=False, default=False)
    _supports_native_interval: bool = attrs.field(init=False, default=False)
    _metadata: MetaData = attrs.field(init=False)
    _t_metadata: Table = attrs.field(init=False)
    _t_tasks: Table = attrs.field(init=False)
    _t_schedules: Table = attrs.field(init=False)
    _t_jobs: Table = attrs.field(init=False)
    _t_job_results: Table = attrs.field(init=False)

    def __attrs_post_init__(self) -> None:
        # Generate the table definitions
        prefix = f"{self.schema}." if self.schema else ""
        self._supports_tzaware_timestamps = self.engine.dialect.name in (
            "postgresql",
            "oracle",
        )
        self._supports_native_interval = self.engine.dialect.name == "postgresql"
        self._metadata = self.get_table_definitions()
        self._t_metadata = self._metadata.tables[prefix + "metadata"]
        self._t_tasks = self._metadata.tables[prefix + "tasks"]
        self._t_schedules = self._metadata.tables[prefix + "schedules"]
        self._t_jobs = self._metadata.tables[prefix + "jobs"]
        self._t_job_results = self._metadata.tables[prefix + "job_results"]

    @classmethod
    def from_url(cls: type[Self], url: str | URL, **options) -> Self:
        """
        Create a new asynchronous SQLAlchemy data store.

        :param url: an SQLAlchemy URL to pass to :func:`~sqlalchemy.create_engine`
            (must use an async dialect like ``asyncpg`` or ``asyncmy``)
        :param options: keyword arguments to pass to the initializer of this class
        :return: the newly created data store

        """
        engine = create_async_engine(url, future=True)
        return cls(engine, **options)

    def _retry(self) -> tenacity.AsyncRetrying:
        def after_attempt(retry_state: tenacity.RetryCallState) -> None:
            self._logger.warning(
                "Temporary data store error (attempt %d): %s",
                retry_state.attempt_number,
                retry_state.outcome.exception(),
            )

        # OSError is raised by asyncpg if it can't connect
        return tenacity.AsyncRetrying(
            stop=self.retry_settings.stop,
            wait=self.retry_settings.wait,
            retry=tenacity.retry_if_exception_type((InterfaceError, OSError)),
            after=after_attempt,
            sleep=anyio.sleep,
            reraise=True,
        )

    @asynccontextmanager
    async def _begin_transaction(
        self,
    ) -> AsyncGenerator[Connection | AsyncConnection, None]:
        # A shielded cancel scope is injected to the exit stack to allow finalization
        # to occur even when the surrounding cancel scope is cancelled
        async with AsyncExitStack() as exit_stack:
            if isinstance(self.engine, AsyncEngine):
                async_cm = self.engine.begin()
                conn = await async_cm.__aenter__()
                exit_stack.enter_context(CancelScope(shield=True))
                exit_stack.push_async_exit(async_cm.__aexit__)
            else:
                cm = self.engine.begin()
                conn = await to_thread.run_sync(cm.__enter__)
                exit_stack.enter_context(CancelScope(shield=True))
                exit_stack.push_async_exit(partial(to_thread.run_sync, cm.__exit__))

            yield conn

    async def _create_metadata(self, conn: Connection | AsyncConnection) -> None:
        if isinstance(conn, AsyncConnection):
            await conn.run_sync(self._metadata.create_all)
        else:
            await to_thread.run_sync(self._metadata.create_all, conn)

    async def _execute(
        self,
        conn: Connection | AsyncConnection,
        statement: Executable,
        parameters: Sequence | Mapping | None = None,
    ):
        if isinstance(conn, AsyncConnection):
            return await conn.execute(statement, parameters)
        else:
            return await to_thread.run_sync(conn.execute, statement, parameters)

    @property
    def _temporary_failure_exceptions(self) -> tuple[type[Exception], ...]:
        # SQlite does not use the network, so it doesn't have "temporary" failures
        if self.engine.dialect.name == "sqlite":
            return ()

        return InterfaceError, OSError

    def _convert_incoming_next_fire_time(self, data: dict[str, Any]) -> dict[str, Any]:
        if not self._supports_tzaware_timestamps:
            utcoffset_minutes = data.pop("next_fire_time_utcoffset", None)
            if utcoffset_minutes is not None:
                tz = timezone(timedelta(minutes=utcoffset_minutes))
                timestamp = data["next_fire_time"] / 1000_000
                data["next_fire_time"] = datetime.fromtimestamp(timestamp, tz=tz)

        return data

    def _convert_outgoing_next_fire_time(self, data: dict[str, Any]) -> dict[str, Any]:
        if not self._supports_tzaware_timestamps:
            next_fire_time = data["next_fire_time"]
            if next_fire_time is not None:
                data["next_fire_time"] = int(next_fire_time.timestamp() * 1000_000)
                data["next_fire_time_utcoffset"] = (
                    next_fire_time.utcoffset().total_seconds() // 60
                )
            else:
                data["next_fire_time_utcoffset"] = None

        return data

    def get_table_definitions(self) -> MetaData:
        if self._supports_tzaware_timestamps:
            timestamp_type: TypeEngine[datetime] = DateTime(timezone=True)
            next_fire_time_tzoffset_columns: tuple[Column, ...] = (
                Column("next_fire_time", timestamp_type, index=True),
            )
        else:
            timestamp_type = EmulatedTimestampTZ()
            next_fire_time_tzoffset_columns = (
                Column("next_fire_time", BigInteger, index=True),
                Column("next_fire_time_utcoffset", SmallInteger),
            )

        if self._supports_native_interval:
            interval_type = Interval(second_precision=6)
        else:
            interval_type = EmulatedInterval()

        metadata = MetaData(schema=self.schema)
        Table("metadata", metadata, Column("schema_version", Integer, nullable=False))
        Table(
            "tasks",
            metadata,
            Column("id", Unicode(500), primary_key=True),
            Column("func", Unicode(500)),
            Column("job_executor", Unicode(500), nullable=False),
            Column("max_running_jobs", Integer),
            Column("misfire_grace_time", interval_type),
            Column("running_jobs", Integer, nullable=False, server_default=literal(0)),
        )
        Table(
            "schedules",
            metadata,
            Column("id", Unicode(500), primary_key=True),
            Column("task_id", Unicode(500), nullable=False, index=True),
            Column("trigger", LargeBinary),
            Column("args", LargeBinary),
            Column("kwargs", LargeBinary),
            Column("paused", Boolean, nullable=False, server_default=literal(False)),
            Column("coalesce", Enum(CoalescePolicy), nullable=False),
            Column("misfire_grace_time", interval_type),
            Column("max_jitter", interval_type),
            *next_fire_time_tzoffset_columns,
            Column("last_fire_time", timestamp_type),
            Column("acquired_by", Unicode(500)),
            Column("acquired_until", timestamp_type),
        )
        Table(
            "jobs",
            metadata,
            Column("id", Uuid, primary_key=True),
            Column("task_id", Unicode(500), nullable=False, index=True),
            Column("args", LargeBinary, nullable=False),
            Column("kwargs", LargeBinary, nullable=False),
            Column("schedule_id", Unicode(500), index=True),
            Column("scheduled_fire_time", timestamp_type),
            Column("jitter", interval_type),
            Column("start_deadline", timestamp_type),
            Column("result_expiration_time", interval_type),
            Column("created_at", timestamp_type, nullable=False),
            Column("started_at", timestamp_type),
            Column("acquired_by", Unicode(500)),
            Column("acquired_until", timestamp_type),
        )
        Table(
            "job_results",
            metadata,
            Column("job_id", Uuid, primary_key=True),
            Column("outcome", Enum(JobOutcome), nullable=False),
            Column("finished_at", timestamp_type, index=True),
            Column("expires_at", timestamp_type, nullable=False, index=True),
            Column("exception", LargeBinary),
            Column("return_value", LargeBinary),
        )
        return metadata

    async def start(
        self, exit_stack: AsyncExitStack, event_broker: EventBroker, logger: Logger
    ) -> None:
        await super().start(exit_stack, event_broker, logger)
        asynclib = sniffio.current_async_library() or "(unknown)"
        if asynclib != "asyncio":
            raise RuntimeError(
                f"This data store requires asyncio; currently running: {asynclib}"
            )

        # Verify that the schema is in place
        async for attempt in self._retry():
            with attempt:
                async with self._begin_transaction() as conn:
                    if self.start_from_scratch:
                        for table in self._metadata.sorted_tables:
                            await self._execute(conn, DropTable(table, if_exists=True))

                    await self._create_metadata(conn)
                    query = select(self._t_metadata.c.schema_version)
                    result = await self._execute(conn, query)
                    version = result.scalar()
                    if version is None:
                        await self._execute(
                            conn, self._t_metadata.insert(), {"schema_version": 1}
                        )
                    elif version > 1:
                        raise RuntimeError(
                            f"Unexpected schema version ({version}); "
                            f"only version 1 is supported by this version of "
                            f"APScheduler"
                        )

        # Find out if the dialect supports UPDATE...RETURNING
        async for attempt in self._retry():
            with attempt:
                update = (
                    self._t_metadata.update()
                    .values(schema_version=self._t_metadata.c.schema_version)
                    .returning(self._t_metadata.c.schema_version)
                )
                async with self._begin_transaction() as conn:
                    try:
                        await self._execute(conn, update)
                    except (CompileError, ProgrammingError):
                        pass  # the support flag is False by default
                    else:
                        self._supports_update_returning = True

    async def _deserialize_schedules(self, result: Result) -> list[Schedule]:
        schedules: list[Schedule] = []
        for row in result:
            try:
                schedules.append(
                    Schedule.unmarshal(
                        self.serializer,
                        self._convert_incoming_next_fire_time(row._asdict()),
                    )
                )
            except SerializationError as exc:
                await self._event_broker.publish(
                    ScheduleDeserializationFailed(schedule_id=row.id, exception=exc)
                )

        return schedules

    async def _deserialize_jobs(self, result: Result) -> list[Job]:
        jobs: list[Job] = []
        for row in result:
            try:
                jobs.append(Job.unmarshal(self.serializer, row._asdict()))
            except SerializationError as exc:
                await self._event_broker.publish(
                    JobDeserializationFailed(job_id=row.id, exception=exc)
                )

        return jobs

    async def add_task(self, task: Task) -> None:
        insert = self._t_tasks.insert().values(
            id=task.id,
            func=task.func,
            job_executor=task.job_executor,
            max_running_jobs=task.max_running_jobs,
            misfire_grace_time=task.misfire_grace_time,
        )
        try:
            async for attempt in self._retry():
                with attempt:
                    async with self._begin_transaction() as conn:
                        await self._execute(conn, insert)
        except IntegrityError:
            update = (
                self._t_tasks.update()
                .values(
                    func=task.func,
                    job_executor=task.job_executor,
                    max_running_jobs=task.max_running_jobs,
                    misfire_grace_time=task.misfire_grace_time,
                )
                .where(self._t_tasks.c.id == task.id)
            )
            async for attempt in self._retry():
                with attempt:
                    async with self._begin_transaction() as conn:
                        await self._execute(conn, update)

            await self._event_broker.publish(TaskUpdated(task_id=task.id))
        else:
            await self._event_broker.publish(TaskAdded(task_id=task.id))

    async def remove_task(self, task_id: str) -> None:
        delete = self._t_tasks.delete().where(self._t_tasks.c.id == task_id)
        async for attempt in self._retry():
            with attempt:
                async with self._begin_transaction() as conn:
                    result = await self._execute(conn, delete)
                    if result.rowcount == 0:
                        raise TaskLookupError(task_id)
                    else:
                        await self._event_broker.publish(TaskRemoved(task_id=task_id))

    async def get_task(self, task_id: str) -> Task:
        query = select(
            self._t_tasks.c.id,
            self._t_tasks.c.func,
            self._t_tasks.c.job_executor,
            self._t_tasks.c.max_running_jobs,
            self._t_tasks.c.misfire_grace_time,
        ).where(self._t_tasks.c.id == task_id)
        async for attempt in self._retry():
            with attempt:
                async with self._begin_transaction() as conn:
                    result = await self._execute(conn, query)
                    row = result.first()

        if row:
            return Task.unmarshal(self.serializer, row._asdict())
        else:
            raise TaskLookupError(task_id)

    async def get_tasks(self) -> list[Task]:
        query = select(
            self._t_tasks.c.id,
            self._t_tasks.c.func,
            self._t_tasks.c.job_executor,
            self._t_tasks.c.max_running_jobs,
            self._t_tasks.c.misfire_grace_time,
        ).order_by(self._t_tasks.c.id)
        async for attempt in self._retry():
            with attempt:
                async with self._begin_transaction() as conn:
                    result = await self._execute(conn, query)
                    tasks = [
                        Task.unmarshal(self.serializer, row._asdict()) for row in result
                    ]

        return tasks

    async def add_schedule(
        self, schedule: Schedule, conflict_policy: ConflictPolicy
    ) -> None:
        event: DataStoreEvent
        values = self._convert_outgoing_next_fire_time(
            schedule.marshal(self.serializer)
        )
        insert = self._t_schedules.insert().values(**values)
        try:
            async for attempt in self._retry():
                with attempt:
                    async with self._begin_transaction() as conn:
                        await self._execute(conn, insert)
        except IntegrityError:
            if conflict_policy is ConflictPolicy.exception:
                raise ConflictingIdError(schedule.id) from None
            elif conflict_policy is ConflictPolicy.replace:
                del values["id"]
                update = (
                    self._t_schedules.update()
                    .where(self._t_schedules.c.id == schedule.id)
                    .values(**values)
                )
                async for attempt in self._retry():
                    with attempt:
                        async with self._begin_transaction() as conn:
                            await self._execute(conn, update)

                event = ScheduleUpdated(
                    schedule_id=schedule.id,
                    task_id=schedule.task_id,
                    next_fire_time=schedule.next_fire_time,
                )
                await self._event_broker.publish(event)
        else:
            event = ScheduleAdded(
                schedule_id=schedule.id,
                task_id=schedule.task_id,
                next_fire_time=schedule.next_fire_time,
            )
            await self._event_broker.publish(event)

    async def remove_schedules(self, ids: Iterable[str]) -> None:
        async for attempt in self._retry():
            with attempt:
                async with self._begin_transaction() as conn:
                    if self._supports_update_returning:
                        delete_returning = (
                            self._t_schedules.delete()
                            .where(self._t_schedules.c.id.in_(ids))
                            .returning(
                                self._t_schedules.c.id, self._t_schedules.c.task_id
                            )
                        )
                        removed_ids: list[tuple[str, str]] = [
                            (row[0], row[1])
                            for row in await self._execute(conn, delete_returning)
                        ]
                    else:
                        query = select(
                            self._t_schedules.c.id, self._t_schedules.c.task_id
                        ).where(self._t_schedules.c.id.in_(ids))
                        ids_to_remove: list[str] = []
                        removed_ids = []
                        for schedule_id, task_id in await self._execute(conn, query):
                            ids_to_remove.append(schedule_id)
                            removed_ids.append((schedule_id, task_id))

                        delete = self._t_schedules.delete().where(
                            self._t_schedules.c.id.in_(ids_to_remove)
                        )
                        await self._execute(conn, delete)

        for schedule_id, task_id in removed_ids:
            await self._event_broker.publish(
                ScheduleRemoved(
                    schedule_id=schedule_id, task_id=task_id, finished=False
                )
            )

    async def get_schedules(self, ids: set[str] | None = None) -> list[Schedule]:
        query = self._t_schedules.select().order_by(self._t_schedules.c.id)
        if ids:
            query = query.where(self._t_schedules.c.id.in_(ids))

        async for attempt in self._retry():
            with attempt:
                async with self._begin_transaction() as conn:
                    result = await self._execute(conn, query)

        return await self._deserialize_schedules(result)

    async def acquire_schedules(self, scheduler_id: str, limit: int) -> list[Schedule]:
        async for attempt in self._retry():
            with attempt:
                async with self._begin_transaction() as conn:
                    now = datetime.now(timezone.utc)
                    acquired_until = now + timedelta(seconds=self.lock_expiration_delay)
                    if self._supports_tzaware_timestamps:
                        comparison = self._t_schedules.c.next_fire_time <= now
                    else:
                        comparison = self._t_schedules.c.next_fire_time <= int(
                            now.timestamp() * 1000_000
                        )

                    schedules_cte = (
                        select(self._t_schedules.c.id)
                        .where(
                            and_(
                                self._t_schedules.c.next_fire_time.isnot(None),
                                comparison,
                                self._t_schedules.c.paused == false(),
                                or_(
                                    self._t_schedules.c.acquired_until.is_(None),
                                    self._t_schedules.c.acquired_until < now,
                                ),
                            )
                        )
                        .order_by(self._t_schedules.c.next_fire_time)
                        .limit(limit)
                        .with_for_update(skip_locked=True)
                        .cte()
                    )
                    subselect = select(schedules_cte.c.id)
                    update = (
                        self._t_schedules.update()
                        .where(self._t_schedules.c.id.in_(subselect))
                        .values(acquired_by=scheduler_id, acquired_until=acquired_until)
                    )
                    if self._supports_update_returning:
                        update = update.returning(*self._t_schedules.columns)
                        result = await self._execute(conn, update)
                    else:
                        await self._execute(conn, update)
                        query = self._t_schedules.select().where(
                            and_(self._t_schedules.c.acquired_by == scheduler_id)
                        )
                        result = await self._execute(conn, query)

                    schedules = await self._deserialize_schedules(result)

        return schedules

    async def release_schedules(
        self, scheduler_id: str, schedules: list[Schedule]
    ) -> None:
        task_ids = {schedule.id: schedule.task_id for schedule in schedules}
        next_fire_times = {
            schedule.id: schedule.next_fire_time for schedule in schedules
        }
        async for attempt in self._retry():
            with attempt:
                async with self._begin_transaction() as conn:
                    update_events: list[ScheduleUpdated] = []
                    finished_schedule_ids: list[str] = []
                    update_args: list[dict[str, Any]] = []
                    for schedule in schedules:
                        try:
                            serialized_trigger = self.serializer.serialize(
                                schedule.trigger
                            )
                        except SerializationError:
                            self._logger.exception(
                                "Error serializing trigger for schedule %r – "
                                "removing from data store",
                                schedule.id,
                            )
                            finished_schedule_ids.append(schedule.id)
                            continue

                        if self._supports_tzaware_timestamps:
                            update_args.append(
                                {
                                    "p_id": schedule.id,
                                    "p_trigger": serialized_trigger,
                                    "p_next_fire_time": schedule.next_fire_time,
                                }
                            )
                        else:
                            if schedule.next_fire_time is not None:
                                timestamp = int(
                                    schedule.next_fire_time.timestamp() * 1000_000
                                )
                                utcoffset = (
                                    schedule.next_fire_time.utcoffset().total_seconds()
                                    // 60
                                )
                            else:
                                timestamp = utcoffset = None

                            update_args.append(
                                {
                                    "p_id": schedule.id,
                                    "p_trigger": serialized_trigger,
                                    "p_next_fire_time": timestamp,
                                    "p_next_fire_time_utcoffset": utcoffset,
                                }
                            )

                    # Update schedules
                    if update_args:
                        extra_values = {}
                        p_id: BindParameter = bindparam("p_id")
                        p_trigger: BindParameter = bindparam("p_trigger")
                        p_next_fire_time: BindParameter = bindparam("p_next_fire_time")
                        if not self._supports_tzaware_timestamps:
                            extra_values["next_fire_time_utcoffset"] = bindparam(
                                "p_next_fire_time_utcoffset"
                            )

                        update = (
                            self._t_schedules.update()
                            .where(
                                and_(
                                    self._t_schedules.c.id == p_id,
                                    self._t_schedules.c.acquired_by == scheduler_id,
                                )
                            )
                            .values(
                                trigger=p_trigger,
                                next_fire_time=p_next_fire_time,
                                acquired_by=None,
                                acquired_until=None,
                                **extra_values,
                            )
                        )
                        # TODO: actually check which rows were updated?
                        await self._execute(conn, update, update_args)
                        updated_ids = list(next_fire_times)

                        for schedule_id in updated_ids:
                            event = ScheduleUpdated(
                                schedule_id=schedule_id,
                                task_id=task_ids[schedule_id],
                                next_fire_time=next_fire_times[schedule_id],
                            )
                            update_events.append(event)

                    # Remove schedules that failed to serialize
                    if finished_schedule_ids:
                        delete = self._t_schedules.delete().where(
                            self._t_schedules.c.id.in_(finished_schedule_ids)
                        )
                        await self._execute(conn, delete)

        for event in update_events:
            await self._event_broker.publish(event)

        for schedule_id in finished_schedule_ids:
            await self._event_broker.publish(
                ScheduleRemoved(
                    schedule_id=schedule_id,
                    task_id=task_ids[schedule_id],
                    finished=True,
                )
            )

    async def get_next_schedule_run_time(self) -> datetime | None:
        columns = [self._t_schedules.c.next_fire_time]
        if not self._supports_tzaware_timestamps:
            columns.append(self._t_schedules.c.next_fire_time_utcoffset)

        statenent = (
            select(*columns)
            .where(
                self._t_schedules.c.next_fire_time.isnot(None),
                self._t_schedules.c.paused == false(),
            )
            .order_by(self._t_schedules.c.next_fire_time)
            .limit(1)
        )
        async for attempt in self._retry():
            with attempt:
                async with self._begin_transaction() as conn:
                    result = await self._execute(conn, statenent)

        if not self._supports_tzaware_timestamps:
            if row := result.first():
                tz = timezone(timedelta(minutes=row[1]))
                return datetime.fromtimestamp(row[0] / 1000_000, tz=tz)
            else:
                return None

        return result.scalar()

    async def add_job(self, job: Job) -> None:
        marshalled = job.marshal(self.serializer)
        insert = self._t_jobs.insert().values(**marshalled)
        async for attempt in self._retry():
            with attempt:
                async with self._begin_transaction() as conn:
                    await self._execute(conn, insert)

        event = JobAdded(
            job_id=job.id,
            task_id=job.task_id,
            schedule_id=job.schedule_id,
        )
        await self._event_broker.publish(event)

    async def get_jobs(self, ids: Iterable[UUID] | None = None) -> list[Job]:
        query = self._t_jobs.select().order_by(self._t_jobs.c.id)
        if ids:
            job_ids = [job_id for job_id in ids]
            query = query.where(self._t_jobs.c.id.in_(job_ids))

        async for attempt in self._retry():
            with attempt:
                async with self._begin_transaction() as conn:
                    result = await self._execute(conn, query)

        return await self._deserialize_jobs(result)

    async def acquire_jobs(
        self, scheduler_id: str, limit: int | None = None
    ) -> list[Job]:
        async for attempt in self._retry():
            with attempt:
                async with self._begin_transaction() as conn:
                    now = datetime.now(timezone.utc)
                    acquired_until = now + timedelta(seconds=self.lock_expiration_delay)
                    query = (
                        self._t_jobs.select()
                        .join(
                            self._t_tasks, self._t_tasks.c.id == self._t_jobs.c.task_id
                        )
                        .where(
                            or_(
                                self._t_jobs.c.acquired_until.is_(None),
                                self._t_jobs.c.acquired_until < now,
                            )
                        )
                        .order_by(self._t_jobs.c.created_at)
                        .with_for_update(skip_locked=True)
                        .limit(limit)
                    )

                    result = await self._execute(conn, query)
                    if not result:
                        return []

                    # Mark the jobs as acquired by this worker
                    jobs = await self._deserialize_jobs(result)
                    task_ids: set[str] = {job.task_id for job in jobs}

                    # Retrieve the limits
                    query = select(
                        self._t_tasks.c.id,
                        self._t_tasks.c.max_running_jobs - self._t_tasks.c.running_jobs,
                    ).where(
                        self._t_tasks.c.max_running_jobs.isnot(None),
                        self._t_tasks.c.id.in_(task_ids),
                    )
                    result = await self._execute(conn, query)
                    job_slots_left: dict[str, int] = dict(result.fetchall())

                    # Filter out jobs that don't have free slots
                    acquired_jobs: list[Job] = []
                    increments: dict[str, int] = defaultdict(lambda: 0)
                    for job in jobs:
                        # Don't acquire the job if there are no free slots left
                        slots_left = job_slots_left.get(job.task_id)
                        if slots_left == 0:
                            continue
                        elif slots_left is not None:
                            job_slots_left[job.task_id] -= 1

                        acquired_jobs.append(job)
                        increments[job.task_id] += 1

                    if acquired_jobs:
                        # Mark the acquired jobs as acquired by this worker
                        acquired_job_ids = [job.id for job in acquired_jobs]
                        update = (
                            self._t_jobs.update()
                            .values(
                                acquired_by=scheduler_id, acquired_until=acquired_until
                            )
                            .where(self._t_jobs.c.id.in_(acquired_job_ids))
                        )
                        await self._execute(conn, update)

                        # Increment the running job counters on each task
                        p_id: BindParameter = bindparam("p_id")
                        p_increment: BindParameter = bindparam("p_increment")
                        params = [
                            {"p_id": task_id, "p_increment": increment}
                            for task_id, increment in increments.items()
                        ]
                        update = (
                            self._t_tasks.update()
                            .values(
                                running_jobs=self._t_tasks.c.running_jobs + p_increment
                            )
                            .where(self._t_tasks.c.id == p_id)
                        )
                        await self._execute(conn, update, params)

        # Publish the appropriate events
        for job in acquired_jobs:
            await self._event_broker.publish(
                JobAcquired.from_job(job, scheduler_id=scheduler_id)
            )

        return acquired_jobs

    async def release_job(self, scheduler_id: str, job: Job, result: JobResult) -> None:
        async for attempt in self._retry():
            with attempt:
                async with self._begin_transaction() as conn:
                    # Record the job result
                    if result.expires_at > result.finished_at:
                        marshalled = result.marshal(self.serializer)
                        insert = self._t_job_results.insert().values(**marshalled)
                        await self._execute(conn, insert)

                    # Decrement the number of running jobs for this task
                    update = (
                        self._t_tasks.update()
                        .values(running_jobs=self._t_tasks.c.running_jobs - 1)
                        .where(self._t_tasks.c.id == job.task_id)
                    )
                    await self._execute(conn, update)

                    # Delete the job
                    delete = self._t_jobs.delete().where(
                        self._t_jobs.c.id == result.job_id
                    )
                    await self._execute(conn, delete)

        # Notify other schedulers
        await self._event_broker.publish(
            JobReleased.from_result(job, result, scheduler_id)
        )

    async def get_job_result(self, job_id: UUID) -> JobResult | None:
        async for attempt in self._retry():
            with attempt:
                async with self._begin_transaction() as conn:
                    # Retrieve the result
                    query = self._t_job_results.select().where(
                        self._t_job_results.c.job_id == job_id
                    )
                    row = (await self._execute(conn, query)).first()

                    # Delete the result
                    delete = self._t_job_results.delete().where(
                        self._t_job_results.c.job_id == job_id
                    )
                    await self._execute(conn, delete)

        return JobResult.unmarshal(self.serializer, row._asdict()) if row else None

    async def cleanup(self) -> None:
        async for attempt in self._retry():
            with attempt:
                async with self._begin_transaction() as conn:
                    # Purge expired job results
                    delete = self._t_job_results.delete().where(
                        self._t_job_results.c.expires_at <= datetime.now(timezone.utc)
                    )
                    await self._execute(conn, delete)

                    # Clean up finished schedules that have no running jobs
                    query = (
                        select(self._t_schedules.c.id, self._t_schedules.c.task_id)
                        .outerjoin(
                            self._t_jobs,
                            self._t_jobs.c.schedule_id == self._t_schedules.c.id,
                        )
                        .where(
                            self._t_schedules.c.next_fire_time.is_(None),
                            self._t_jobs.c.id.is_(None),
                        )
                    )
                    results = await self._execute(conn, query)
                    if finished_schedule_ids := dict(results.all()):
                        delete = self._t_schedules.delete().where(
                            self._t_schedules.c.id.in_(finished_schedule_ids)
                        )
                        await self._execute(conn, delete)

                for schedule_id, task_id in finished_schedule_ids.items():
                    await self._event_broker.publish(
                        ScheduleRemoved(
                            schedule_id=schedule_id,
                            task_id=task_id,
                            finished=True,
                        )
                    )
