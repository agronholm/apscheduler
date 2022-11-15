from __future__ import annotations

import sys
from collections import defaultdict
from contextlib import AsyncExitStack
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable
from uuid import UUID

import anyio
import attrs
import sniffio
import tenacity as tenacity
from asyncpg import Connection, Pool, create_pool
from asyncpg.protocol.protocol import Record

from .. import (
    ConflictPolicy,
    DataStoreEvent,
    Job,
    JobAcquired,
    JobAdded,
    JobDeserializationFailed,
    JobResult,
    Schedule,
    ScheduleAdded,
    ScheduleDeserializationFailed,
    ScheduleRemoved,
    ScheduleUpdated,
    SerializationError,
    Task,
    TaskAdded,
    TaskLookupError,
    TaskRemoved,
    TaskUpdated,
)
from ..abc import EventBroker
from ..marshalling import callable_to_ref
from .base import BaseExternalDataStore

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


class MyRecord(Record):
    def __getattr__(self, name):
        return self[name]


@attrs.define(eq=False)
class AsyncPgDataStore(BaseExternalDataStore):
    dsn: str
    pool: Pool | None = attrs.field(default=None)
    schema: str | None = attrs.field(default=None)
    _tables: list[str] = attrs.field(
        default=["metadata", "tasks", "schedules", "jobs", "job_results"]
    )
    _dbtypes: list[str] = ["coalescepolicy", "joboutcome"]

    @classmethod
    async def from_dsn(
        cls, dsn: str, schema: str, start_from_scratch: bool, **pool_options
    ) -> Self:
        self = AsyncPgDataStore(
            dsn=dsn,
            schema=schema,
            start_from_scratch=start_from_scratch,
            **pool_options,
        )
        self.pool = await create_pool(
            dsn=dsn, server_settings={"search_path": schema}, **pool_options
        )
        return self

    def _retry(self) -> tenacity.AsyncRetrying:
        def after_attempt(self, retry_state: tenacity.RetryCallState) -> None:
            self._logger.warning(
                "Temporary data store error (attempt %d): %s",
                retry_state.attempt_number,
                retry_state.outcome.exception(),
            )

        # OSError is raised by asyncpg if it can't connect
        return tenacity.AsyncRetrying(
            stop=self.retry_settings.stop,
            wait=self.retry_settings.wait,
            retry=tenacity.retry_if_exception_type(OSError),
            after=after_attempt,
            sleep=anyio.sleep,
            reraise=True,
        )

    async def _create_schema(self, conn: Connection):
        with open(Path(__file__).parent / "asyncpg.sql") as f:
            query = f.read()
        async with conn.transaction():
            await conn.execute(f"create schema if not exists {self.schema}")
            await conn.execute(query)

    async def start(
        self, exit_stack: AsyncExitStack, event_broker: EventBroker
    ) -> None:
        await super().start(exit_stack, event_broker)
        asynclib = sniffio.current_async_library() or "(unknown)"
        if asynclib != "asyncio":
            raise RuntimeError(
                f"This data store requires asyncio; currently running: {asynclib}"
            )

        # Verify that the schema is in place
        async for attempt in self._retry():
            with attempt:
                async with self.pool.acquire() as conn:
                    if self.start_from_scratch:
                        for table_name in self._tables:
                            await conn.execute(f"drop table if exists {table_name}")
                        for dbtype in self._dbtypes:
                            await conn.execute(f"drop type if exists {dbtype}")
                    await self._create_schema(conn)
                    version = await conn.fetchval("select schema_version from metadata")
                    if version is None:
                        r = await conn.execute(
                            "insert into metadata(schema_version) values (1)"
                        )
                        print(f"{table_name}: {r}")
                    elif version > 1:
                        raise RuntimeError(
                            f"Unexpected schema version ({version}); "
                            f"only version 1 is supported by this version of "
                            f"APScheduler"
                        )

    async def add_task(self, task: Task) -> None:
        query = """insert into tasks (id, func, executor, max_running_jobs, misfire_grace_time)
        values ($1, $2, $3, $4, $5)
        on conflict (id) do update set func=$2, executor=$3, max_running_jobs=$4, misfire_grace_time=$5
        where tasks.id=$1
        returning case when xmax::text::int > 0 then 'updated' else 'inserted' end
        """  # noqa: E501
        async with self.pool.acquire() as conn:
            r = await conn.fetchval(
                query,
                task.id,
                callable_to_ref(task.func),
                task.executor,
                task.max_running_jobs,
                task.misfire_grace_time,
            )
        if r == "inserted":
            await self._event_broker.publish(TaskAdded(task_id=task.id))
        elif r == "updated":
            await self._event_broker.publish(TaskUpdated(task_id=task.id))

    async def remove_task(self, task_id: str) -> None:
        delete = """delete from tasks where id=$1"""
        async for attempt in self._retry():
            with attempt:
                async with self.pool.acquire() as conn:
                    result = await conn.fetch(delete, task_id)
                    if len(result) == 0:
                        raise TaskLookupError(task_id)
                    else:
                        await self._event_broker.publish(TaskRemoved(task_id=task_id))

    async def get_task(self, task_id: str) -> Task:
        query = """select id, func, executor,
        max_running_jobs, state, misfire_grace_time
        from tasks where id=$1"""
        async for attempt in self._retry():
            with attempt:
                async with self.pool.acquire() as conn:
                    row = await conn.fetchrow(query, task_id, record_class=MyRecord)

        if row:
            return Task.unmarshal(self.serializer, {k: v for k, v in row.items()})
        else:
            raise TaskLookupError(task_id)

    async def get_tasks(self) -> list[Task]:
        query = """select id, func, executor,
        max_running_jobs, state, misfire_grace_time
        from tasks order by id"""
        async with self.pool.acquire() as conn:
            result = await conn.fetch(query, record_class=MyRecord)
            tasks = [
                Task.unmarshal(self.serializer, {k: v for k, v in row.items()})
                for row in result
            ]
            return tasks

    async def _deserialize_schedules(self, result: list[MyRecord]) -> list[Schedule]:
        schedules: list[Schedule] = []
        for row in result:
            try:
                schedules.append(
                    Schedule.unmarshal(self.serializer, {k: v for k, v in row.items()})
                )
            except SerializationError as exc:
                await self._event_broker.publish(
                    ScheduleDeserializationFailed(schedule_id=row["id"], exception=exc)
                )

        return schedules

    async def _deserialize_jobs(self, result: list[MyRecord]) -> list[Job]:
        jobs: list[Job] = []
        for row in result:
            try:
                jobs.append(
                    Job.unmarshal(self.serializer, {k: v for k, v in row.items()})
                )
            except SerializationError as exc:
                await self._event_broker.publish(
                    JobDeserializationFailed(job_id=row["id"], exception=exc)
                )

        return jobs

    async def get_schedules(self, ids: set[str] | None = None) -> list[Schedule]:
        async for attempt in self._retry():
            with attempt:
                async with self.pool.acquire() as conn:
                    async with conn.transaction():
                        if ids:
                            query = """select * from schedules where id=any($1) order by id"""  # noqa: E501
                            result = await conn.fetch(query, ids, record_class=MyRecord)
                        else:
                            query = """select * from schedules order by id"""
                            result = await conn.fetch(query, record_class=MyRecord)
                        return await self._deserialize_schedules(result)

    async def add_schedule(
        self, schedule: Schedule, conflict_policy: ConflictPolicy
    ) -> None:
        event: DataStoreEvent
        values = schedule.marshal(self.serializer)
        query = """insert into schedules(id, task_id, trigger,
        args, kwargs, coalesce,
        misfire_grace_time, max_jitter,
        tags, next_fire_time, last_fire_time,
        acquired_by, acquired_until)
        values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
        on conflict (id) do update set task_id=$2, trigger=$3,
        args=$4, kwargs=$5, coalesce=$6,
        misfire_grace_time=$7, max_jitter=$8,
        tags=$9, next_fire_time=$10, last_fire_time=$11,
        acquired_by=$12, acquired_until=$13
        where schedules.id=$1
        returning case when xmax::text::int > 0 then 'updated' else 'inserted' end
        """
        async for attempt in self._retry():
            with attempt:
                async with self.pool.acquire() as conn:
                    r = await conn.fetchval(
                        query,
                        values["id"],
                        values["task_id"],
                        values["trigger"],
                        values["args"],
                        values["kwargs"],
                        values["coalesce"].name,
                        values.get("misfire_grace_time", "null"),
                        values.get("max_jitter", "null"),
                        values["tags"],
                        values["next_fire_time"],
                        values["last_fire_time"],
                        values.get("acquired_by"),
                        values.get("acquired_until"),
                    )
                    if r == "inserted":
                        event = ScheduleAdded(
                            schedule_id=schedule.id,
                            next_fire_time=schedule.next_fire_time,
                        )
                        await self._event_broker.publish(event)
                    elif r == "updated":
                        event = ScheduleUpdated(
                            schedule_id=schedule.id,
                            next_fire_time=schedule.next_fire_time,
                        )
                        await self._event_broker.publish(event)

    async def remove_schedules(self, ids: Iterable[str]) -> None:
        async for attempt in self._retry():
            with attempt:
                async with self.pool.acquire() as conn:
                    removed_ids = [
                        r.id
                        for r in await conn.fetch(
                            """delete from schedules where id=any($1) returning id""",
                            ids,
                            record_class=MyRecord,
                        )
                    ]

        for schedule_id in removed_ids:
            await self._event_broker.publish(ScheduleRemoved(schedule_id=schedule_id))

    async def acquire_schedules(self, scheduler_id: str, limit: int) -> list[Schedule]:
        async for attempt in self._retry():
            with attempt:
                async with self.pool.acquire() as conn:
                    now = datetime.now(timezone.utc)
                    acquired_until = now + timedelta(seconds=self.lock_expiration_delay)
                    query = """with schedules_cte as (
                    select id from schedules
                    where next_fire_time is not null and next_fire_time <= $1 and ( acquired_until is null or acquired_until < $1)
                    order by next_fire_time
                    limit $2
                    for update skip locked)
                    update schedules set acquired_by=$3, acquired_until=$4 where id=any(select id from schedules_cte)
                    returning *
                    """  # noqa: E501
                    result = await conn.fetch(
                        query,
                        now,
                        limit,
                        scheduler_id,
                        acquired_until,
                        record_class=MyRecord,
                    )
                    schedules = await self._deserialize_schedules(result)

        return schedules

    async def release_schedules(
        self, scheduler_id: str, schedules: list[Schedule]
    ) -> None:
        pass
        async for attempt in self._retry():
            with attempt:
                async with self.pool.acquire() as conn:
                    async with conn.transaction():
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
                            # TODO: actually check which rows were updated?
                            query = """update schedules set trigger=$3, next_fire_time=$4, acquired_by=$5, acquired_until=$6
                            where id = $1 and acquired_by = $2
                            returning id"""  # noqa: E501
                            await conn.executemany(
                                query,
                                [
                                    (
                                        u["p_id"],
                                        scheduler_id,
                                        u["p_trigger"],
                                        u["p_next_fire_time"],
                                        None,
                                        None,
                                    )
                                    for u in update_args
                                ],
                            )
                            next_fire_times = {
                                arg["p_id"]: arg["p_next_fire_time"]
                                for arg in update_args
                            }
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
                            query = """delete from schedules where id=any($1)"""
                            await conn.execute(query, finished_schedule_ids)

        for event in update_events:
            await self._event_broker.publish(event)

        for schedule_id in finished_schedule_ids:
            await self._event_broker.publish(ScheduleRemoved(schedule_id=schedule_id))

    async def get_next_schedule_run_time(self) -> datetime | None:
        statement = """ select next_fire_time from schedules
        where next_fire_time is not null
        order by next_fire_time
        limit 1"""
        async for attempt in self._retry():
            with attempt:
                async with self.pool.acquire() as conn:
                    result = await conn.fetchval(statement)
                    return result

    async def add_job(self, job: Job) -> None:
        marshalled = job.marshal(self.serializer)
        insert = """insert into jobs(id, task_id, args,
         kwargs, schedule_id, scheduled_fire_time, jitter,
         start_deadline, result_expiration_time, tags,
         created_at, started_at) values($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)"""  # noqa: E501
        async for attempt in self._retry():
            with attempt:
                async with self.pool.acquire() as conn:
                    await conn.execute(
                        insert,
                        marshalled["id"],
                        marshalled["task_id"],
                        marshalled["args"],
                        marshalled["kwargs"],
                        marshalled["schedule_id"],
                        marshalled["scheduled_fire_time"],
                        marshalled["jitter"],
                        marshalled["start_deadline"],
                        marshalled["result_expiration_time"],
                        marshalled["tags"],
                        marshalled["created_at"],
                        marshalled["started_at"],
                    )

        event = JobAdded(
            job_id=job.id,
            task_id=job.task_id,
            schedule_id=job.schedule_id,
            tags=job.tags,
        )
        await self._event_broker.publish(event)

    async def get_jobs(self, ids: Iterable[UUID] | None = None) -> list[Job]:
        pass

    async def acquire_jobs(self, worker_id: str, limit: int | None = None) -> list[Job]:
        async for attempt in self._retry():
            with attempt:
                async with self.pool.acquire() as conn:
                    async with conn.transaction():
                        now = datetime.now(timezone.utc)
                        acquired_until = now + timedelta(
                            seconds=self.lock_expiration_delay
                        )
                        query = """select j.* from jobs j join tasks t on t.id=j.task_id
                        where acquired_until is null or acquired_until < $1
                        order by created_at
                        limit $2
                        for update skip locked
                        """

                        result = await conn.fetch(
                            query, now, limit, record_class=MyRecord
                        )
                        if not result:
                            return []

                        # Mark the jobs as acquired by this worker
                        jobs = await self._deserialize_jobs(result)
                        task_ids: set[str] = {job.task_id for job in jobs}

                        # Retrieve the limits
                        query = """select id, max_running_jobs - running_jobs
                        from tasks where max_running_jobs is not null and id=any($1)"""
                        result = await conn.fetch(query, task_ids)
                        job_slots_left: dict[str, int] = dict(result)

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
                            update = """update jobs set acquired_by=$1,
                            acquired_until=$2 where id=any($3)"""
                            await conn.execute(
                                update, worker_id, acquired_until, acquired_job_ids
                            )

                            # Increment the running job counters on each task
                            params = [
                                {"p_id": task_id, "p_increment": increment}
                                for task_id, increment in increments.items()
                            ]
                            update = """update tasks set
                            running_jobs=running_jobs+$1 where id=$2
                            """
                            await conn.executemany(
                                update, [(p["p_increment"], p["p_id"]) for p in params]
                            )

        # Publish the appropriate events
        for job in acquired_jobs:
            await self._event_broker.publish(
                JobAcquired(job_id=job.id, worker_id=worker_id)
            )

        return acquired_jobs

    async def release_job(
        self, worker_id: str, task_id: str, result: JobResult
    ) -> None:
        async for attempt in self._retry():
            with attempt:
                async with self.pool.acquire() as conn:
                    async with conn.transaction():
                        # Record the job result
                        if result.expires_at > result.finished_at:
                            marshalled = result.marshal(self.serializer)
                            insert = """insert into job_results(job_id, outcome,
                            finished_at, expires_at, exception, return_value)
                            values ($1, $2, $3, $4, $5, $6)"""
                            await conn.execute(
                                insert,
                                marshalled["job_id"],
                                marshalled["outcome"].name,
                                marshalled["finished_at"],
                                marshalled["expires_at"],
                                marshalled.get("exception"),
                                marshalled.get("return_value"),
                            )

                        # Decrement the number of running jobs for this task
                        update = """update tasks set running_jobs=running_jobs-1 where id=$1"""  # noqa: E501
                        await conn.execute(update, task_id)

                        # Delete the job
                        delete = """delete from jobs where id=$1"""
                        await conn.execute(delete, result.job_id)

    async def get_job_result(self, job_id: UUID) -> JobResult | None:
        async for attempt in self._retry():
            with attempt:
                async with self.pool.acquire() as conn:
                    async with conn.transaction():
                        # Retrieve the result
                        query = """select * from job_results where job_id=$1"""
                        row = await conn.fetchrow(query, job_id, record_class=MyRecord)

                        # Delete the result
                        delete = """delete from job_results where job_id=$1"""
                        await conn.execute(delete, job_id)

                        return (
                            JobResult.unmarshal(
                                self.serializer, {k: v for k, v in row.items()}
                            )
                            if row
                            else None
                        )
