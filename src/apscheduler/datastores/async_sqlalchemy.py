from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable
from uuid import UUID

import anyio
import attrs
import sniffio
import tenacity
from sqlalchemy import and_, bindparam, or_, select
from sqlalchemy.engine import URL, Result
from sqlalchemy.exc import IntegrityError, InterfaceError
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.ext.asyncio.engine import AsyncEngine
from sqlalchemy.sql.ddl import DropTable
from sqlalchemy.sql.elements import BindParameter

from .._enums import ConflictPolicy
from .._events import (
    DataStoreEvent,
    JobAcquired,
    JobAdded,
    JobDeserializationFailed,
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
from ..abc import AsyncEventBroker
from ..marshalling import callable_to_ref
from .base import BaseAsyncDataStore
from .sqlalchemy import _BaseSQLAlchemyDataStore


@attrs.define(eq=False)
class AsyncSQLAlchemyDataStore(_BaseSQLAlchemyDataStore, BaseAsyncDataStore):
    """
    Uses a relational database to store data.

    When started, this data store creates the appropriate tables on the given database
    if they're not already present.

    Operations are retried (in accordance to ``retry_settings``) when an operation
    raises :exc:`sqlalchemy.OperationalError`.

    This store has been tested to work with PostgreSQL (asyncpg driver) and MySQL
    (asyncmy driver).

    :param engine: an asynchronous SQLAlchemy engine
    :param schema: a database schema name to use, if not the default
    :param serializer: the serializer used to (de)serialize tasks, schedules and jobs
        for storage
    :param lock_expiration_delay: maximum amount of time (in seconds) that a scheduler
        or worker can keep a lock on a schedule or task
    :param retry_settings: Tenacity settings for retrying operations in case of a
        database connecitivty problem
    :param start_from_scratch: erase all existing data during startup (useful for test
        suites)
    """

    engine: AsyncEngine

    @classmethod
    def from_url(cls, url: str | URL, **options) -> AsyncSQLAlchemyDataStore:
        """
        Create a new asynchronous SQLAlchemy data store.

        :param url: an SQLAlchemy URL to pass to :func:`~sqlalchemy.create_engine`
            (must use an async dialect like ``asyncpg`` or ``asyncmy``)
        :param kwargs: keyword arguments to pass to the initializer of this class
        :return: the newly created data store

        """
        engine = create_async_engine(url, future=True)
        return cls(engine, **options)

    def _retry(self) -> tenacity.AsyncRetrying:
        # OSError is raised by asyncpg if it can't connect
        return tenacity.AsyncRetrying(
            stop=self.retry_settings.stop,
            wait=self.retry_settings.wait,
            retry=tenacity.retry_if_exception_type((InterfaceError, OSError)),
            after=self._after_attempt,
            sleep=anyio.sleep,
            reraise=True,
        )

    async def start(self, event_broker: AsyncEventBroker) -> None:
        await super().start(event_broker)

        asynclib = sniffio.current_async_library() or "(unknown)"
        if asynclib != "asyncio":
            raise RuntimeError(
                f"This data store requires asyncio; currently running: {asynclib}"
            )

        # Verify that the schema is in place
        async for attempt in self._retry():
            with attempt:
                async with self.engine.begin() as conn:
                    if self.start_from_scratch:
                        for table in self._metadata.sorted_tables:
                            await conn.execute(DropTable(table, if_exists=True))

                    await conn.run_sync(self._metadata.create_all)
                    query = select(self.t_metadata.c.schema_version)
                    result = await conn.execute(query)
                    version = result.scalar()
                    if version is None:
                        await conn.execute(
                            self.t_metadata.insert(values={"schema_version": 1})
                        )
                    elif version > 1:
                        raise RuntimeError(
                            f"Unexpected schema version ({version}); "
                            f"only version 1 is supported by this version of "
                            f"APScheduler"
                        )

    async def _deserialize_schedules(self, result: Result) -> list[Schedule]:
        schedules: list[Schedule] = []
        for row in result:
            try:
                schedules.append(Schedule.unmarshal(self.serializer, row._asdict()))
            except SerializationError as exc:
                await self._events.publish(
                    ScheduleDeserializationFailed(schedule_id=row["id"], exception=exc)
                )

        return schedules

    async def _deserialize_jobs(self, result: Result) -> list[Job]:
        jobs: list[Job] = []
        for row in result:
            try:
                jobs.append(Job.unmarshal(self.serializer, row._asdict()))
            except SerializationError as exc:
                await self._events.publish(
                    JobDeserializationFailed(job_id=row["id"], exception=exc)
                )

        return jobs

    async def add_task(self, task: Task) -> None:
        insert = self.t_tasks.insert().values(
            id=task.id,
            func=callable_to_ref(task.func),
            max_running_jobs=task.max_running_jobs,
            misfire_grace_time=task.misfire_grace_time,
        )
        try:
            async for attempt in self._retry():
                with attempt:
                    async with self.engine.begin() as conn:
                        await conn.execute(insert)
        except IntegrityError:
            update = (
                self.t_tasks.update()
                .values(
                    func=callable_to_ref(task.func),
                    max_running_jobs=task.max_running_jobs,
                    misfire_grace_time=task.misfire_grace_time,
                )
                .where(self.t_tasks.c.id == task.id)
            )
            async for attempt in self._retry():
                with attempt:
                    async with self.engine.begin() as conn:
                        await conn.execute(update)

            await self._events.publish(TaskUpdated(task_id=task.id))
        else:
            await self._events.publish(TaskAdded(task_id=task.id))

    async def remove_task(self, task_id: str) -> None:
        delete = self.t_tasks.delete().where(self.t_tasks.c.id == task_id)
        async for attempt in self._retry():
            with attempt:
                async with self.engine.begin() as conn:
                    result = await conn.execute(delete)
                    if result.rowcount == 0:
                        raise TaskLookupError(task_id)
                    else:
                        await self._events.publish(TaskRemoved(task_id=task_id))

    async def get_task(self, task_id: str) -> Task:
        query = select(
            [
                self.t_tasks.c.id,
                self.t_tasks.c.func,
                self.t_tasks.c.max_running_jobs,
                self.t_tasks.c.state,
                self.t_tasks.c.misfire_grace_time,
            ]
        ).where(self.t_tasks.c.id == task_id)
        async for attempt in self._retry():
            with attempt:
                async with self.engine.begin() as conn:
                    result = await conn.execute(query)
                    row = result.first()

        if row:
            return Task.unmarshal(self.serializer, row._asdict())
        else:
            raise TaskLookupError(task_id)

    async def get_tasks(self) -> list[Task]:
        query = select(
            [
                self.t_tasks.c.id,
                self.t_tasks.c.func,
                self.t_tasks.c.max_running_jobs,
                self.t_tasks.c.state,
                self.t_tasks.c.misfire_grace_time,
            ]
        ).order_by(self.t_tasks.c.id)
        async for attempt in self._retry():
            with attempt:
                async with self.engine.begin() as conn:
                    result = await conn.execute(query)
                    tasks = [
                        Task.unmarshal(self.serializer, row._asdict()) for row in result
                    ]
                    return tasks

    async def add_schedule(
        self, schedule: Schedule, conflict_policy: ConflictPolicy
    ) -> None:
        event: DataStoreEvent
        values = schedule.marshal(self.serializer)
        insert = self.t_schedules.insert().values(**values)
        try:
            async for attempt in self._retry():
                with attempt:
                    async with self.engine.begin() as conn:
                        await conn.execute(insert)
        except IntegrityError:
            if conflict_policy is ConflictPolicy.exception:
                raise ConflictingIdError(schedule.id) from None
            elif conflict_policy is ConflictPolicy.replace:
                del values["id"]
                update = (
                    self.t_schedules.update()
                    .where(self.t_schedules.c.id == schedule.id)
                    .values(**values)
                )
                async for attempt in self._retry():
                    with attempt:
                        async with self.engine.begin() as conn:
                            await conn.execute(update)

                event = ScheduleUpdated(
                    schedule_id=schedule.id, next_fire_time=schedule.next_fire_time
                )
                await self._events.publish(event)
        else:
            event = ScheduleAdded(
                schedule_id=schedule.id, next_fire_time=schedule.next_fire_time
            )
            await self._events.publish(event)

    async def remove_schedules(self, ids: Iterable[str]) -> None:
        async for attempt in self._retry():
            with attempt:
                async with self.engine.begin() as conn:
                    delete = self.t_schedules.delete().where(
                        self.t_schedules.c.id.in_(ids)
                    )
                    if self._supports_update_returning:
                        delete = delete.returning(self.t_schedules.c.id)
                        removed_ids: Iterable[str] = [
                            row[0] for row in await conn.execute(delete)
                        ]
                    else:
                        # TODO: actually check which rows were deleted?
                        await conn.execute(delete)
                        removed_ids = ids

        for schedule_id in removed_ids:
            await self._events.publish(ScheduleRemoved(schedule_id=schedule_id))

    async def get_schedules(self, ids: set[str] | None = None) -> list[Schedule]:
        query = self.t_schedules.select().order_by(self.t_schedules.c.id)
        if ids:
            query = query.where(self.t_schedules.c.id.in_(ids))

        async for attempt in self._retry():
            with attempt:
                async with self.engine.begin() as conn:
                    result = await conn.execute(query)
                    return await self._deserialize_schedules(result)

    async def acquire_schedules(self, scheduler_id: str, limit: int) -> list[Schedule]:
        async for attempt in self._retry():
            with attempt:
                async with self.engine.begin() as conn:
                    now = datetime.now(timezone.utc)
                    acquired_until = now + timedelta(seconds=self.lock_expiration_delay)
                    schedules_cte = (
                        select(self.t_schedules.c.id)
                        .where(
                            and_(
                                self.t_schedules.c.next_fire_time.isnot(None),
                                self.t_schedules.c.next_fire_time <= now,
                                or_(
                                    self.t_schedules.c.acquired_until.is_(None),
                                    self.t_schedules.c.acquired_until < now,
                                ),
                            )
                        )
                        .order_by(self.t_schedules.c.next_fire_time)
                        .limit(limit)
                        .with_for_update(skip_locked=True)
                        .cte()
                    )
                    subselect = select([schedules_cte.c.id])
                    update = (
                        self.t_schedules.update()
                        .where(self.t_schedules.c.id.in_(subselect))
                        .values(acquired_by=scheduler_id, acquired_until=acquired_until)
                    )
                    if self._supports_update_returning:
                        update = update.returning(*self.t_schedules.columns)
                        result = await conn.execute(update)
                    else:
                        await conn.execute(update)
                        query = self.t_schedules.select().where(
                            and_(self.t_schedules.c.acquired_by == scheduler_id)
                        )
                        result = await conn.execute(query)

                    schedules = await self._deserialize_schedules(result)

        return schedules

    async def release_schedules(
        self, scheduler_id: str, schedules: list[Schedule]
    ) -> None:
        async for attempt in self._retry():
            with attempt:
                async with self.engine.begin() as conn:
                    update_events: list[ScheduleUpdated] = []
                    finished_schedule_ids: list[str] = []
                    update_args: list[dict[str, Any]] = []
                    for schedule in schedules:
                        if schedule.next_fire_time is not None:
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

                            update_args.append(
                                {
                                    "p_id": schedule.id,
                                    "p_trigger": serialized_trigger,
                                    "p_next_fire_time": schedule.next_fire_time,
                                }
                            )
                        else:
                            finished_schedule_ids.append(schedule.id)

                    # Update schedules that have a next fire time
                    if update_args:
                        p_id: BindParameter = bindparam("p_id")
                        p_trigger: BindParameter = bindparam("p_trigger")
                        p_next_fire_time: BindParameter = bindparam("p_next_fire_time")
                        update = (
                            self.t_schedules.update()
                            .where(
                                and_(
                                    self.t_schedules.c.id == p_id,
                                    self.t_schedules.c.acquired_by == scheduler_id,
                                )
                            )
                            .values(
                                trigger=p_trigger,
                                next_fire_time=p_next_fire_time,
                                acquired_by=None,
                                acquired_until=None,
                            )
                        )
                        next_fire_times = {
                            arg["p_id"]: arg["p_next_fire_time"] for arg in update_args
                        }
                        # TODO: actually check which rows were updated?
                        await conn.execute(update, update_args)
                        updated_ids = list(next_fire_times)

                        for schedule_id in updated_ids:
                            event = ScheduleUpdated(
                                schedule_id=schedule_id,
                                next_fire_time=next_fire_times[schedule_id],
                            )
                            update_events.append(event)

                    # Remove schedules that have no next fire time or failed to
                    # serialize
                    if finished_schedule_ids:
                        delete = self.t_schedules.delete().where(
                            self.t_schedules.c.id.in_(finished_schedule_ids)
                        )
                        await conn.execute(delete)

        for event in update_events:
            await self._events.publish(event)

        for schedule_id in finished_schedule_ids:
            await self._events.publish(ScheduleRemoved(schedule_id=schedule_id))

    async def get_next_schedule_run_time(self) -> datetime | None:
        statenent = (
            select(self.t_schedules.c.next_fire_time)
            .where(self.t_schedules.c.next_fire_time.isnot(None))
            .order_by(self.t_schedules.c.next_fire_time)
            .limit(1)
        )
        async for attempt in self._retry():
            with attempt:
                async with self.engine.begin() as conn:
                    result = await conn.execute(statenent)
                    return result.scalar()

    async def add_job(self, job: Job) -> None:
        marshalled = job.marshal(self.serializer)
        insert = self.t_jobs.insert().values(**marshalled)
        async for attempt in self._retry():
            with attempt:
                async with self.engine.begin() as conn:
                    await conn.execute(insert)

        event = JobAdded(
            job_id=job.id,
            task_id=job.task_id,
            schedule_id=job.schedule_id,
            tags=job.tags,
        )
        await self._events.publish(event)

    async def get_jobs(self, ids: Iterable[UUID] | None = None) -> list[Job]:
        query = self.t_jobs.select().order_by(self.t_jobs.c.id)
        if ids:
            job_ids = [job_id for job_id in ids]
            query = query.where(self.t_jobs.c.id.in_(job_ids))

        async for attempt in self._retry():
            with attempt:
                async with self.engine.begin() as conn:
                    result = await conn.execute(query)
                    return await self._deserialize_jobs(result)

    async def acquire_jobs(self, worker_id: str, limit: int | None = None) -> list[Job]:
        async for attempt in self._retry():
            with attempt:
                async with self.engine.begin() as conn:
                    now = datetime.now(timezone.utc)
                    acquired_until = now + timedelta(seconds=self.lock_expiration_delay)
                    query = (
                        self.t_jobs.select()
                        .join(self.t_tasks, self.t_tasks.c.id == self.t_jobs.c.task_id)
                        .where(
                            or_(
                                self.t_jobs.c.acquired_until.is_(None),
                                self.t_jobs.c.acquired_until < now,
                            )
                        )
                        .order_by(self.t_jobs.c.created_at)
                        .with_for_update(skip_locked=True)
                        .limit(limit)
                    )

                    result = await conn.execute(query)
                    if not result:
                        return []

                    # Mark the jobs as acquired by this worker
                    jobs = await self._deserialize_jobs(result)
                    task_ids: set[str] = {job.task_id for job in jobs}

                    # Retrieve the limits
                    query = select(
                        [
                            self.t_tasks.c.id,
                            self.t_tasks.c.max_running_jobs
                            - self.t_tasks.c.running_jobs,
                        ]
                    ).where(
                        self.t_tasks.c.max_running_jobs.isnot(None),
                        self.t_tasks.c.id.in_(task_ids),
                    )
                    result = await conn.execute(query)
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
                            self.t_jobs.update()
                            .values(
                                acquired_by=worker_id, acquired_until=acquired_until
                            )
                            .where(self.t_jobs.c.id.in_(acquired_job_ids))
                        )
                        await conn.execute(update)

                        # Increment the running job counters on each task
                        p_id: BindParameter = bindparam("p_id")
                        p_increment: BindParameter = bindparam("p_increment")
                        params = [
                            {"p_id": task_id, "p_increment": increment}
                            for task_id, increment in increments.items()
                        ]
                        update = (
                            self.t_tasks.update()
                            .values(
                                running_jobs=self.t_tasks.c.running_jobs + p_increment
                            )
                            .where(self.t_tasks.c.id == p_id)
                        )
                        await conn.execute(update, params)

        # Publish the appropriate events
        for job in acquired_jobs:
            await self._events.publish(JobAcquired(job_id=job.id, worker_id=worker_id))

        return acquired_jobs

    async def release_job(
        self, worker_id: str, task_id: str, result: JobResult
    ) -> None:
        async for attempt in self._retry():
            with attempt:
                async with self.engine.begin() as conn:
                    # Record the job result
                    if result.expires_at > result.finished_at:
                        marshalled = result.marshal(self.serializer)
                        insert = self.t_job_results.insert().values(**marshalled)
                        await conn.execute(insert)

                    # Decrement the number of running jobs for this task
                    update = (
                        self.t_tasks.update()
                        .values(running_jobs=self.t_tasks.c.running_jobs - 1)
                        .where(self.t_tasks.c.id == task_id)
                    )
                    await conn.execute(update)

                    # Delete the job
                    delete = self.t_jobs.delete().where(
                        self.t_jobs.c.id == result.job_id
                    )
                    await conn.execute(delete)

    async def get_job_result(self, job_id: UUID) -> JobResult | None:
        async for attempt in self._retry():
            with attempt:
                async with self.engine.begin() as conn:
                    # Retrieve the result
                    query = self.t_job_results.select().where(
                        self.t_job_results.c.job_id == job_id
                    )
                    row = (await conn.execute(query)).first()

                    # Delete the result
                    delete = self.t_job_results.delete().where(
                        self.t_job_results.c.job_id == job_id
                    )
                    await conn.execute(delete)

                    return (
                        JobResult.unmarshal(self.serializer, row._asdict())
                        if row
                        else None
                    )
