from __future__ import annotations

import operator
import sys
from collections.abc import AsyncGenerator, AsyncIterator, Iterable, Mapping, Sequence
from contextlib import AsyncExitStack, asynccontextmanager
from datetime import datetime, timedelta, timezone
from logging import Logger
from typing import Any, Callable, ClassVar, Generic, TypeVar, cast
from uuid import UUID

import attrs
import pymongo
from anyio import to_thread
from attrs.validators import instance_of
from bson import CodecOptions, UuidRepresentation
from bson.codec_options import TypeEncoder, TypeRegistry
from pymongo import ASCENDING, DeleteOne, MongoClient, UpdateOne
from pymongo.client_session import ClientSession
from pymongo.collection import Collection
from pymongo.cursor import Cursor
from pymongo.errors import ConnectionFailure, DuplicateKeyError

from .._enums import CoalescePolicy, ConflictPolicy, JobOutcome
from .._events import (
    DataStoreEvent,
    JobAcquired,
    JobAdded,
    JobReleased,
    ScheduleAdded,
    ScheduleRemoved,
    ScheduleUpdated,
    TaskAdded,
    TaskRemoved,
    TaskUpdated,
)
from .._exceptions import (
    ConflictingIdError,
    DeserializationError,
    SerializationError,
    TaskLookupError,
)
from .._structures import Job, JobResult, Schedule, ScheduleResult, Task
from .._utils import create_repr
from ..abc import EventBroker
from .base import BaseExternalDataStore

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

T = TypeVar("T", bound=Mapping[str, Any])


class CustomEncoder(TypeEncoder):
    def __init__(self, python_type: type, encoder: Callable):
        self._python_type = python_type
        self._encoder = encoder

    @property
    def python_type(self) -> type:
        return self._python_type

    def transform_python(self, value: Any) -> Any:
        return self._encoder(value)


def marshal_timestamp(timestamp: datetime | None, key: str) -> Mapping[str, Any]:
    if timestamp is None:
        return {key: None, key + "_utcoffset": None}

    return {
        key: timestamp.timestamp(),
        key + "_utcoffset": cast(timedelta, timestamp.utcoffset()).total_seconds()
        // 60,
    }


def marshal_document(document: dict[str, Any]) -> None:
    if "id" in document:
        document["_id"] = document.pop("id")

    for key, value in list(document.items()):
        if isinstance(value, datetime):
            document.update(marshal_timestamp(document[key], key))


def unmarshal_timestamps(document: dict[str, Any]) -> None:
    for key in list(document):
        if key.endswith("_utcoffset") and (value := document.pop(key)) is not None:
            offset = timedelta(seconds=value * 60)
            tzinfo = timezone(offset)
            time_micro = document[key[:-10]]
            document[key[:-10]] = datetime.fromtimestamp(time_micro, tzinfo)


@attrs.define(eq=False, repr=False)
class AsyncCursor(Generic[T]):
    cursor: Cursor[T]

    def __aiter__(self) -> AsyncIterator[T]:
        return self

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await to_thread.run_sync(self.cursor.close)

    def __next__(self) -> T:
        try:
            return next(self.cursor)
        except StopIteration:
            raise StopAsyncIteration from None

    async def __anext__(self) -> T:
        return await to_thread.run_sync(next, self)

    @classmethod
    async def create(cls, func: Callable[..., Cursor[T]]) -> AsyncCursor[T]:
        cursor = await to_thread.run_sync(func)
        return AsyncCursor(cursor)


@attrs.define(eq=False, repr=False)
class MongoDBDataStore(BaseExternalDataStore):
    """
    Uses a MongoDB server to store data.

    When started, this data store creates the appropriate indexes on the given database
    if they're not already present.

    Operations are retried (in accordance to ``retry_settings``) when an operation
    raises :exc:`pymongo.errors.ConnectionFailure`.

    :param client_or_uri: a PyMongo client or a MongoDB connection URI
    :param database: name of the database to use

    .. note:: The data store will not manage the life cycle of any client instance
        passed to it, so you need to close the client afterwards when you're done with
        it.

    .. note:: Datetimes are stored as integers along with their UTC offsets instead of
        BSON datetimes due to the BSON datetimes only being accurate to the millisecond
        while Python datetimes are accurate to the microsecond.
    """

    client_or_uri: MongoClient | str = attrs.field(
        validator=instance_of((MongoClient, str))
    )
    database: str = attrs.field(
        default="apscheduler", kw_only=True, validator=instance_of(str)
    )

    _client: MongoClient = attrs.field(init=False)
    _close_on_exit: bool = attrs.field(init=False, default=False)
    _task_attrs: ClassVar[list[str]] = [field.name for field in attrs.fields(Task)]
    _schedule_attrs: ClassVar[list[str]] = [
        field.name for field in attrs.fields(Schedule)
    ]
    _job_attrs: ClassVar[list[str]] = [field.name for field in attrs.fields(Job)]
    _tasks: Collection = attrs.field(init=False)
    _schedules: Collection = attrs.field(init=False)
    _jobs: Collection = attrs.field(init=False)
    _jobs_results: Collection = attrs.field(init=False)

    @property
    def _temporary_failure_exceptions(self) -> tuple[type[Exception], ...]:
        return (ConnectionFailure,)

    def __attrs_post_init__(self) -> None:
        if isinstance(self.client_or_uri, str):
            self._client = MongoClient(self.client_or_uri)
            self._close_on_exit = True
        else:
            self._client = self.client_or_uri

        type_registry = TypeRegistry(
            [
                CustomEncoder(timedelta, timedelta.total_seconds),
                CustomEncoder(ConflictPolicy, operator.attrgetter("name")),
                CustomEncoder(CoalescePolicy, operator.attrgetter("name")),
                CustomEncoder(JobOutcome, operator.attrgetter("name")),
            ]
        )
        codec_options: CodecOptions = CodecOptions(
            type_registry=type_registry,
            uuid_representation=UuidRepresentation.STANDARD,
        )
        database = self._client.get_database(self.database, codec_options=codec_options)
        self._tasks = database["tasks"]
        self._schedules = database["schedules"]
        self._jobs = database["jobs"]
        self._jobs_results = database["job_results"]

    def __repr__(self) -> str:
        server_descriptions = self._client.topology_description.server_descriptions()
        return create_repr(self, host=list(server_descriptions))

    def _initialize(self) -> None:
        with self._client.start_session() as session:
            if self.start_from_scratch:
                self._tasks.delete_many({}, session=session)
                self._schedules.delete_many({}, session=session)
                self._jobs.delete_many({}, session=session)
                self._jobs_results.delete_many({}, session=session)

            self._schedules.create_index("task_id", session=session)
            self._schedules.create_index("next_fire_time", session=session)
            self._schedules.create_index("acquired_by", session=session)
            self._jobs.create_index("task_id", session=session)
            self._jobs.create_index("schedule_id", session=session)
            self._jobs.create_index("created_at", session=session)
            self._jobs.create_index("acquired_by", session=session)
            self._jobs_results.create_index("expires_at", session=session)

    @asynccontextmanager
    async def _get_session(self) -> AsyncGenerator[ClientSession, None]:
        session = await to_thread.run_sync(self._client.start_session)
        try:
            yield session
        finally:
            await to_thread.run_sync(session.end_session)

    async def start(
        self, exit_stack: AsyncExitStack, event_broker: EventBroker, logger: Logger
    ) -> None:
        if self._close_on_exit:
            exit_stack.push_async_callback(to_thread.run_sync, self._client.close)

        await super().start(exit_stack, event_broker, logger)
        server_info = await to_thread.run_sync(self._client.server_info)
        if server_info["versionArray"] < [4, 0]:
            raise RuntimeError(
                f"MongoDB server must be at least v4.0; current version = "
                f"{server_info['version']}"
            )

        async for attempt in self._retry():
            with attempt:
                await to_thread.run_sync(self._initialize)

    async def add_task(self, task: Task) -> None:
        async for attempt in self._retry():
            with attempt:
                previous = await to_thread.run_sync(
                    lambda: self._tasks.find_one_and_update(
                        {"_id": task.id},
                        {"$set": task.marshal(self.serializer)},
                        upsert=True,
                    )
                )

        if previous:
            await self._event_broker.publish(TaskUpdated(task_id=task.id))
        else:
            await self._event_broker.publish(TaskAdded(task_id=task.id))

    async def remove_task(self, task_id: str) -> None:
        async for attempt in self._retry():
            with attempt:
                if not await to_thread.run_sync(
                    self._tasks.find_one_and_delete, {"_id": task_id}
                ):
                    raise TaskLookupError(task_id)

        await self._event_broker.publish(TaskRemoved(task_id=task_id))

    async def get_task(self, task_id: str) -> Task:
        async for attempt in self._retry():
            with attempt:
                document = await to_thread.run_sync(
                    lambda: self._tasks.find_one(
                        {"_id": task_id}, projection=self._task_attrs
                    )
                )

        if not document:
            raise TaskLookupError(task_id)

        document["id"] = document.pop("_id")
        task = Task.unmarshal(self.serializer, document)
        return task

    async def get_tasks(self) -> list[Task]:
        async for attempt in self._retry():
            with attempt:
                tasks: list[Task] = []
                async with await AsyncCursor.create(
                    lambda: self._tasks.find(
                        projection=self._task_attrs, sort=[("_id", pymongo.ASCENDING)]
                    )
                ) as cursor:
                    async for document in cursor:
                        document["id"] = document.pop("_id")
                        tasks.append(Task.unmarshal(self.serializer, document))

        return tasks

    async def get_schedules(self, ids: set[str] | None = None) -> list[Schedule]:
        filters = {"_id": {"$in": list(ids)}} if ids is not None else {}
        async for attempt in self._retry():
            with attempt:
                schedules: list[Schedule] = []
                async with await AsyncCursor.create(
                    lambda: self._schedules.find(filters).sort("_id")
                ) as cursor:
                    async for document in cursor:
                        document["id"] = document.pop("_id")
                        unmarshal_timestamps(document)
                        try:
                            schedule = Schedule.unmarshal(self.serializer, document)
                        except DeserializationError:
                            self._logger.warning(
                                "Failed to deserialize schedule %r", document["_id"]
                            )
                            continue

                        schedules.append(schedule)

        return schedules

    async def add_schedule(
        self, schedule: Schedule, conflict_policy: ConflictPolicy
    ) -> None:
        event: DataStoreEvent
        document = schedule.marshal(self.serializer)
        marshal_document(document)
        try:
            async for attempt in self._retry():
                with attempt:
                    await to_thread.run_sync(self._schedules.insert_one, document)
        except DuplicateKeyError:
            if conflict_policy is ConflictPolicy.exception:
                raise ConflictingIdError(schedule.id) from None
            elif conflict_policy is ConflictPolicy.replace:
                async for attempt in self._retry():
                    with attempt:
                        await to_thread.run_sync(
                            self._schedules.replace_one,
                            {"_id": schedule.id},
                            document,
                            True,
                        )

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
        filters = {"_id": {"$in": list(ids)}} if ids is not None else {}
        async for attempt in self._retry():
            with attempt, self._client.start_session() as session:
                async with await AsyncCursor.create(
                    lambda: self._schedules.find(
                        filters, projection=["_id", "task_id"], session=session
                    )
                ) as cursor:
                    new_ids = [(doc["_id"], doc["task_id"]) async for doc in cursor]
                    if new_ids:
                        self._schedules.delete_many(filters, session=session)

        for schedule_id, task_id in new_ids:
            await self._event_broker.publish(
                ScheduleRemoved(
                    schedule_id=schedule_id, task_id=task_id, finished=False
                )
            )

    async def acquire_schedules(
        self, scheduler_id: str, lease_duration: timedelta, limit: int
    ) -> list[Schedule]:
        schedules: list[Schedule] = []

        # Fetch up to {limit} schedules
        while len(schedules) < limit:
            async for attempt in self._retry():
                with attempt, self._client.start_session() as session:
                    now = datetime.now(timezone.utc)
                    async with await AsyncCursor.create(
                        lambda: self._schedules.find(
                            {
                                "next_fire_time": {"$lte": now.timestamp()},
                                "$and": [
                                    {
                                        "$or": [
                                            {"paused": {"$exists": False}},
                                            {"paused": False},
                                        ]
                                    },
                                    {
                                        "$or": [
                                            {"acquired_by": scheduler_id},
                                            {"acquired_until": {"$exists": False}},
                                            {
                                                "acquired_until": {
                                                    "$lt": now.timestamp()
                                                }
                                            },
                                        ]
                                    },
                                ],
                            },
                            session=session,
                        )
                        .sort("next_fire_time")
                        .limit(limit - len(schedules))
                    ) as cursor:
                        documents = [doc async for doc in cursor]

                    # Bail out if there are no more schedules to be acquired
                    if not documents:
                        return schedules

                    now = datetime.now(timezone.utc)
                    acquired_until = now + lease_duration
                    schedule_ids = [doc["_id"] for doc in documents]
                    result = await to_thread.run_sync(
                        lambda: self._schedules.update_many(
                            {
                                "_id": {"$in": schedule_ids},
                                "$or": [
                                    {"acquired_until": {"$exists": False}},
                                    {"acquired_until": {"$lt": now.timestamp()}},
                                ],
                            },
                            {
                                "$set": {
                                    "acquired_by": scheduler_id,
                                    **marshal_timestamp(
                                        acquired_until, "acquired_until"
                                    ),
                                }
                            },
                        )
                    )

                    # If the number of modified schedules was smaller than expected,
                    # manually check which ones were successfully acquired
                    if result.modified_count != len(schedule_ids):
                        async with await AsyncCursor.create(
                            lambda: self._schedules.find(
                                {
                                    "_id": {"$in": schedule_ids},
                                    "acquired_by": scheduler_id,
                                },
                                sort=[("created_at", ASCENDING)],
                                projection=["_id"],
                                session=session,
                            )
                        ) as cursor:
                            acquired_schedule_ids = {doc["_id"] async for doc in cursor}
                            documents = [
                                doc
                                for doc in documents
                                if doc["_id"] in acquired_schedule_ids
                            ]

                    for doc in documents:
                        # Deserialize the schedule
                        doc["id"] = doc.pop("_id")
                        unmarshal_timestamps(doc)
                        schedules.append(Schedule.unmarshal(self.serializer, doc))

                return schedules

    async def release_schedules(
        self, scheduler_id: str, results: Sequence[ScheduleResult]
    ) -> None:
        updated_schedules: list[tuple[str, datetime | None]] = []
        finished_schedule_ids: list[str] = []
        task_ids = {result.schedule_id: result.task_id for result in results}

        requests: list[UpdateOne | DeleteOne] = []
        for result in results:
            filters = {"_id": result.schedule_id, "acquired_by": scheduler_id}
            try:
                serialized_trigger = self.serializer.serialize(result.trigger)
            except SerializationError:
                self._logger.exception(
                    "Error serializing schedule %r – removing from data store",
                    result.schedule_id,
                )
                requests.append(DeleteOne(filters))
                finished_schedule_ids.append(result.schedule_id)
                continue

            update = {
                "$unset": {
                    "acquired_by": True,
                    "acquired_until": True,
                    "acquired_until_utcoffset": True,
                },
                "$set": {
                    "trigger": serialized_trigger,
                    **marshal_timestamp(result.last_fire_time, "last_fire_time"),
                    **marshal_timestamp(result.next_fire_time, "next_fire_time"),
                },
            }
            requests.append(UpdateOne(filters, update))
            updated_schedules.append((result.schedule_id, result.next_fire_time))

        if requests:
            async for attempt in self._retry():
                with attempt, self._client.start_session() as session:
                    await to_thread.run_sync(
                        lambda: self._schedules.bulk_write(
                            requests, ordered=False, session=session
                        )
                    )

        for schedule_id, next_fire_time in updated_schedules:
            event = ScheduleUpdated(
                schedule_id=schedule_id,
                task_id=task_ids[schedule_id],
                next_fire_time=next_fire_time,
            )
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
        async for attempt in self._retry():
            with attempt:
                document = await to_thread.run_sync(
                    lambda: self._schedules.find_one(
                        {"next_fire_time": {"$ne": None}},
                        projection=["next_fire_time", "next_fire_time_utcoffset"],
                        sort=[("next_fire_time", ASCENDING)],
                    )
                )

        if document:
            unmarshal_timestamps(document)
            return document["next_fire_time"]
        else:
            return None

    async def add_job(self, job: Job) -> None:
        document = job.marshal(self.serializer)
        marshal_document(document)
        async for attempt in self._retry():
            with attempt:
                await to_thread.run_sync(self._jobs.insert_one, document)

        event = JobAdded(
            job_id=job.id,
            task_id=job.task_id,
            schedule_id=job.schedule_id,
        )
        await self._event_broker.publish(event)

    async def get_jobs(self, ids: Iterable[UUID] | None = None) -> list[Job]:
        filters = {"_id": {"$in": list(ids)}} if ids is not None else {}
        async for attempt in self._retry():
            with attempt:
                jobs: list[Job] = []
                async with await AsyncCursor.create(
                    lambda: self._jobs.find(filters).sort("_id")
                ) as cursor:
                    async for document in cursor:
                        document["id"] = document.pop("_id")
                        unmarshal_timestamps(document)
                        try:
                            job = Job.unmarshal(self.serializer, document)
                        except DeserializationError:
                            self._logger.warning(
                                "Failed to deserialize job %r", document["id"]
                            )
                            continue

                        jobs.append(job)

        return jobs

    async def acquire_jobs(
        self, scheduler_id: str, lease_duration: timedelta, limit: int | None = None
    ) -> list[Job]:
        events: list[JobAcquired | JobReleased] = []
        async for attempt in self._retry():
            async with AsyncExitStack() as exit_stack:
                exit_stack.enter_context(attempt)
                session = await exit_stack.enter_async_context(self._get_session())

                # Fetch up to {limit} jobs
                now = datetime.now(timezone.utc)
                async with await AsyncCursor.create(
                    lambda: self._jobs.find(
                        {
                            "$or": [
                                {"acquired_until": {"$exists": False}},
                                {"acquired_until": {"$lt": now.timestamp()}},
                            ]
                        },
                        sort=[("created_at", ASCENDING)],
                        limit=limit,
                        session=session,
                    )
                ) as cursor:
                    documents = [doc async for doc in cursor]

                # Mark them as acquired by this scheduler
                acquired_until = now + lease_duration
                job_ids = [doc["_id"] for doc in documents]
                result = await to_thread.run_sync(
                    lambda: self._jobs.update_many(
                        {
                            "_id": {"$in": job_ids},
                            "$or": [
                                {"acquired_until": {"$exists": False}},
                                {"acquired_until": {"$lt": now.timestamp()}},
                            ],
                        },
                        {
                            "$set": {
                                "acquired_by": scheduler_id,
                                **marshal_timestamp(acquired_until, "acquired_until"),
                            }
                        },
                    )
                )

                # If the number of modified jobs was smaller than expected, manually
                # check which jobs were successfully acquired
                if result.modified_count != len(job_ids):
                    async with await AsyncCursor.create(
                        lambda: self._jobs.find(
                            {
                                "_id": {"$in": job_ids},
                                "acquired_by": scheduler_id,
                            },
                            sort=[("created_at", ASCENDING)],
                            projection=["_id"],
                            session=session,
                        )
                    ) as cursor:
                        acquired_job_ids = {doc["_id"] async for doc in cursor}
                        documents = [
                            doc for doc in documents if doc["_id"] in acquired_job_ids
                        ]

                acquired_jobs: list[Job] = []
                skipped_job_ids: list[UUID] = []
                for doc in documents:
                    # Deserialize the job
                    doc["id"] = doc.pop("_id")
                    unmarshal_timestamps(doc)
                    try:
                        job = Job.unmarshal(self.serializer, doc)
                    except DeserializationError as exc:
                        # Deserialization failed, so record the exception as the job
                        # result
                        result = JobResult(
                            job_id=doc["id"],
                            outcome=JobOutcome.missed_start_deadline,
                            finished_at=now,
                            expires_at=now
                            + timedelta(seconds=doc["result_expiration_time"]),
                            exception=exc,
                        )
                        events.append(
                            await self._release_job(
                                session,
                                result,
                                scheduler_id,
                                doc["task_id"],
                                doc["schedule_id"],
                                doc["scheduled_fire_time"],
                                decrement_running_job_count=False,
                            )
                        )
                        continue

                    # Discard the job if its start deadline has passed
                    if job.start_deadline and job.start_deadline < now:
                        result = JobResult.from_job(
                            job,
                            JobOutcome.missed_start_deadline,
                            finished_at=now,
                        )
                        events.append(
                            await self._release_job(
                                session,
                                result,
                                scheduler_id,
                                job.task_id,
                                job.schedule_id,
                                job.scheduled_fire_time,
                                decrement_running_job_count=False,
                            )
                        )
                        continue

                    # Try to increment the task's running jobs count
                    update_task_result = await to_thread.run_sync(
                        lambda: self._tasks.update_one(
                            {
                                "_id": job.task_id,
                                "$or": [
                                    {"max_running_jobs": None},
                                    {
                                        "$expr": {
                                            "$gt": [
                                                "$max_running_jobs",
                                                "$running_jobs",
                                            ]
                                        }
                                    },
                                ],
                            },
                            {"$inc": {"running_jobs": 1}},
                            session=session,
                        )
                    )
                    if not update_task_result.matched_count:
                        self._logger.debug(
                            "Skipping job %s because task %r has the maximum number of "
                            "jobs already running",
                            job.id,
                            job.task_id,
                        )
                        skipped_job_ids.append(job.id)
                        continue

                    job.acquired_by = scheduler_id
                    job.acquired_until = now + lease_duration
                    acquired_jobs.append(job)
                    events.append(JobAcquired.from_job(job, scheduler_id=scheduler_id))

                # Release jobs skipped due to max job slots being reached
                if skipped_job_ids:
                    await to_thread.run_sync(
                        lambda: self._jobs.update_many(
                            {
                                "_id": {"$in": skipped_job_ids},
                                "acquired_by": scheduler_id,
                            },
                            {
                                "$unset": {
                                    "acquired_by": True,
                                    "acquired_until": True,
                                    "acquired_until_utcoffset": True,
                                },
                            },
                        )
                    )

        # Publish the appropriate events
        for event in events:
            await self._event_broker.publish(event)

        return acquired_jobs

    async def _release_job(
        self,
        session: ClientSession,
        result: JobResult,
        scheduler_id: str,
        task_id: str,
        schedule_id: str | None = None,
        scheduled_fire_time: datetime | None = None,
        *,
        decrement_running_job_count: bool = True,
    ) -> JobReleased:
        # Record the job result
        if result.expires_at > result.finished_at:
            document = result.marshal(self.serializer)
            document["_id"] = document.pop("job_id")
            marshal_document(document)
            self._jobs_results.insert_one(document, session=session)

        # Delete the job
        await to_thread.run_sync(
            lambda: self._jobs.delete_one({"_id": result.job_id}, session=session)
        )

        # Decrement the running jobs counter if the job had been successfully acquired
        if decrement_running_job_count:
            await to_thread.run_sync(
                lambda: self._tasks.find_one_and_update(
                    {"_id": task_id},
                    {"$inc": {"running_jobs": -1}},
                    session=session,
                )
            )

        # Notify other schedulers
        return JobReleased.from_result(
            result, scheduler_id, task_id, schedule_id, scheduled_fire_time
        )

    async def release_job(self, scheduler_id: str, job: Job, result: JobResult) -> None:
        async for attempt in self._retry():
            with attempt, self._client.start_session() as session:
                event = await self._release_job(
                    session,
                    result,
                    scheduler_id,
                    job.task_id,
                    job.schedule_id,
                    job.scheduled_fire_time,
                )

                # Notify other schedulers
                await self._event_broker.publish(event)

    async def get_job_result(self, job_id: UUID) -> JobResult | None:
        async for attempt in self._retry():
            with attempt:
                document = await to_thread.run_sync(
                    lambda: self._jobs_results.find_one_and_delete({"_id": job_id})
                )

        if document:
            document["job_id"] = document.pop("_id")
            unmarshal_timestamps(document)
            return JobResult.unmarshal(self.serializer, document)
        else:
            return None

    async def extend_acquired_schedule_leases(
        self, scheduler_id: str, schedule_ids: set[str], duration: timedelta
    ) -> None:
        async for attempt in self._retry():
            with attempt, self._client.start_session() as session:
                new_acquired_until = (datetime.now(timezone.utc) + duration).timestamp()
                await to_thread.run_sync(
                    lambda: self._schedules.update_many(
                        filter={
                            "acquired_by": scheduler_id,
                            "_id": {"$in": list(schedule_ids)},
                        },
                        update={"$set": {"acquired_until": new_acquired_until}},
                        session=session,
                    )
                )

    async def extend_acquired_job_leases(
        self, scheduler_id: str, job_ids: set[UUID], duration: timedelta
    ) -> None:
        async for attempt in self._retry():
            with attempt, self._client.start_session() as session:
                new_acquired_until = (datetime.now(timezone.utc) + duration).timestamp()
                await to_thread.run_sync(
                    lambda: self._jobs.update_many(
                        filter={
                            "acquired_by": scheduler_id,
                            "_id": {"$in": list(job_ids)},
                        },
                        update={"$set": {"acquired_until": new_acquired_until}},
                        session=session,
                    )
                )

    async def reap_abandoned_jobs(self, scheduler_id: str) -> None:
        async for attempt in self._retry():
            events: list[JobReleased] = []
            with attempt, self._client.start_session() as session:
                async with await AsyncCursor.create(
                    lambda: self._jobs.find(
                        filter={"acquired_by": scheduler_id},
                        sort=[("created_at", ASCENDING)],
                    )
                ) as cursor:
                    async for doc in cursor:
                        doc["id"] = doc.pop("_id")
                        unmarshal_timestamps(doc)
                        job = Job.unmarshal(
                            self.serializer, {**doc, "args": (), "kwargs": {}}
                        )
                        result = JobResult.from_job(job, JobOutcome.abandoned)
                        event = await self._release_job(
                            session,
                            result,
                            scheduler_id,
                            job.task_id,
                            job.schedule_id,
                            job.scheduled_fire_time,
                        )
                        events.append(event)

            for event in events:
                await self._event_broker.publish(event)

    async def cleanup(self) -> None:
        events: list[JobReleased | ScheduleRemoved] = []
        async for attempt in self._retry():
            with attempt, self._client.start_session() as session:
                # Purge expired job results
                now = datetime.now(timezone.utc)
                await to_thread.run_sync(
                    lambda: self._jobs_results.delete_many(
                        {"expires_at": {"$lte": now.timestamp()}}, session=session
                    )
                )

                # Find finished schedules
                async with await AsyncCursor.create(
                    lambda: self._schedules.find(
                        {"next_fire_time": None},
                        projection=["_id", "task_id"],
                        session=session,
                    )
                ) as cursor:
                    if finished_schedules := {
                        item["_id"]: item["task_id"] async for item in cursor
                    }:
                        # Find distinct schedule IDs of jobs associated with these
                        # schedules
                        for schedule_id in await to_thread.run_sync(
                            lambda: self._jobs.distinct(
                                "schedule_id",
                                {"schedule_id": {"$in": list(finished_schedules)}},
                                session=session,
                            )
                        ):
                            finished_schedules.pop(schedule_id)

                # Finish any jobs whose leases have expired
                filters = {"acquired_until": {"$lt": now.timestamp()}}
                async with await AsyncCursor.create(
                    lambda: self._jobs.find(
                        filters,
                        projection=[
                            "_id",
                            "acquired_by",
                            "task_id",
                            "schedule_id",
                            "scheduled_fire_time",
                            "scheduled_fire_time_utcoffset",
                            "result_expiration_time",
                        ],
                    )
                ) as cursor:
                    async for doc in cursor:
                        unmarshal_timestamps(doc)
                        result = JobResult(
                            job_id=doc["_id"],
                            outcome=JobOutcome.abandoned,
                            finished_at=now,
                            expires_at=now
                            + timedelta(seconds=doc["result_expiration_time"]),
                        )
                        events.append(
                            await self._release_job(
                                session,
                                result,
                                doc["acquired_by"],
                                doc["task_id"],
                                doc["schedule_id"],
                                doc["scheduled_fire_time"],
                            )
                        )

                # Delete finished schedules that not having any associated jobs
                if finished_schedules:
                    await to_thread.run_sync(
                        lambda: self._schedules.delete_many(
                            {"_id": {"$in": list(finished_schedules)}},
                            session=session,
                        )
                    )
                    for schedule_id, task_id in finished_schedules.items():
                        events.append(
                            ScheduleRemoved(
                                schedule_id=schedule_id,
                                task_id=task_id,
                                finished=True,
                            )
                        )

        # Publish any events produced from the operations
        for event in events:
            await self._event_broker.publish(event)
