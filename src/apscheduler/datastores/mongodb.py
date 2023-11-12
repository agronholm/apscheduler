from __future__ import annotations

import operator
from collections import defaultdict
from collections.abc import Mapping
from contextlib import AsyncExitStack
from datetime import datetime, timedelta, timezone
from logging import Logger
from typing import Any, Callable, ClassVar, Iterable
from uuid import UUID

import attrs
import pymongo
from anyio import to_thread
from attrs.validators import instance_of
from bson import CodecOptions, UuidRepresentation
from bson.codec_options import TypeEncoder, TypeRegistry
from pymongo import ASCENDING, DeleteOne, MongoClient, UpdateOne
from pymongo.collection import Collection
from pymongo.errors import ConnectionFailure, DuplicateKeyError

from .._enums import CoalescePolicy, ConflictPolicy, JobOutcome
from .._events import (
    DataStoreEvent,
    JobAcquired,
    JobAdded,
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
from .._structures import Job, JobResult, Schedule, Task
from ..abc import EventBroker
from .base import BaseExternalDataStore


class CustomEncoder(TypeEncoder):
    def __init__(self, python_type: type, encoder: Callable):
        self._python_type = python_type
        self._encoder = encoder

    @property
    def python_type(self) -> type:
        return self._python_type

    def transform_python(self, value: Any) -> Any:
        return self._encoder(value)


def marshal_timestamp(timestamp: datetime, key: str) -> Mapping[str, Any]:
    return {
        key: timestamp.timestamp(),
        key + "_utcoffset": timestamp.utcoffset().total_seconds() // 60,
    }


def marshal_document(document: dict[str, Any]) -> None:
    if "id" in document:
        document["_id"] = document.pop("id")

    for key, value in list(document.items()):
        if isinstance(value, datetime):
            document.update(marshal_timestamp(document[key], key))


def unmarshal_timestamps(document: dict[str, Any]) -> None:
    for key in list(document):
        if key.endswith("_utcoffset"):
            offset = timedelta(seconds=document.pop(key) * 60)
            tzinfo = timezone(offset)
            time_micro = document[key[:-10]]
            document[key[:-10]] = datetime.fromtimestamp(time_micro, tzinfo)


@attrs.define(eq=False)
class MongoDBDataStore(BaseExternalDataStore):
    """
    Uses a MongoDB server to store data.

    When started, this data store creates the appropriate indexes on the given database
    if they're not already present.

    Operations are retried (in accordance to ``retry_settings``) when an operation
    raises :exc:`pymongo.errors.ConnectionFailure`.

    :param client: a PyMongo client
    :param database: name of the database to use

    .. note:: Datetimes are stored as integers along with their UTC offsets instead of
        BSON datetimes due to the BSON datetimes only being accurate to the millisecond
        while Python datetimes are accurate to the microsecond.
    """

    client: MongoClient = attrs.field(validator=instance_of(MongoClient))
    database: str = attrs.field(default="apscheduler", kw_only=True)

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
        type_registry = TypeRegistry(
            [
                CustomEncoder(timedelta, timedelta.total_seconds),
                CustomEncoder(ConflictPolicy, operator.attrgetter("name")),
                CustomEncoder(CoalescePolicy, operator.attrgetter("name")),
                CustomEncoder(JobOutcome, operator.attrgetter("name")),
            ]
        )
        codec_options = CodecOptions(
            type_registry=type_registry,
            uuid_representation=UuidRepresentation.STANDARD,
        )
        database = self.client.get_database(self.database, codec_options=codec_options)
        self._tasks = database["tasks"]
        self._schedules = database["schedules"]
        self._jobs = database["jobs"]
        self._jobs_results = database["job_results"]

    @classmethod
    def from_url(cls, uri: str, **options) -> MongoDBDataStore:
        client = MongoClient(uri)
        return cls(client, **options)

    def _initialize(self) -> None:
        with self.client.start_session() as session:
            if self.start_from_scratch:
                self._tasks.delete_many({}, session=session)
                self._schedules.delete_many({}, session=session)
                self._jobs.delete_many({}, session=session)
                self._jobs_results.delete_many({}, session=session)

            self._schedules.create_index("next_fire_time", session=session)
            self._jobs.create_index("task_id", session=session)
            self._jobs.create_index("created_at", session=session)
            self._jobs_results.create_index("finished_at", session=session)
            self._jobs_results.create_index("expires_at", session=session)

    async def start(
        self, exit_stack: AsyncExitStack, event_broker: EventBroker, logger: Logger
    ) -> None:
        await super().start(exit_stack, event_broker, logger)
        server_info = await to_thread.run_sync(self.client.server_info)
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
                        {
                            "$set": task.marshal(self.serializer),
                            "$setOnInsert": {"running_jobs": 0},
                        },
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
                for document in self._tasks.find(
                    projection=self._task_attrs, sort=[("_id", pymongo.ASCENDING)]
                ):
                    document["id"] = document.pop("_id")
                    tasks.append(Task.unmarshal(self.serializer, document))

        return tasks

    async def get_schedules(self, ids: set[str] | None = None) -> list[Schedule]:
        filters = {"_id": {"$in": list(ids)}} if ids is not None else {}
        async for attempt in self._retry():
            with attempt:
                schedules: list[Schedule] = []
                cursor = self._schedules.find(filters).sort("_id")
                for document in cursor:
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
                    self._schedules.insert_one(document)
        except DuplicateKeyError:
            if conflict_policy is ConflictPolicy.exception:
                raise ConflictingIdError(schedule.id) from None
            elif conflict_policy is ConflictPolicy.replace:
                async for attempt in self._retry():
                    with attempt:
                        self._schedules.replace_one(
                            {"_id": schedule.id}, document, True
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
            with attempt, self.client.start_session() as session:
                cursor = self._schedules.find(
                    filters, projection=["_id", "task_id"], session=session
                )
                ids = [(doc["_id"], doc["task_id"]) for doc in cursor]
                if ids:
                    self._schedules.delete_many(filters, session=session)

        for schedule_id, task_id in ids:
            await self._event_broker.publish(
                ScheduleRemoved(
                    schedule_id=schedule_id, task_id=task_id, finished=False
                )
            )

    async def acquire_schedules(self, scheduler_id: str, limit: int) -> list[Schedule]:
        async for attempt in self._retry():
            with attempt, self.client.start_session() as session:
                schedules: list[Schedule] = []
                now = datetime.now(timezone.utc).timestamp()
                cursor = (
                    self._schedules.find(
                        {
                            "next_fire_time": {"$lte": now},
                            "$or": [
                                {"acquired_until": {"$exists": False}},
                                {"acquired_until": {"$lt": now}},
                            ],
                        },
                        session=session,
                    )
                    .sort("next_fire_time")
                    .limit(limit)
                )
                for document in cursor:
                    document["id"] = document.pop("_id")
                    unmarshal_timestamps(document)
                    schedule = Schedule.unmarshal(self.serializer, document)
                    schedules.append(schedule)

                if schedules:
                    now = datetime.now(timezone.utc)
                    acquired_until = now + timedelta(seconds=self.lock_expiration_delay)
                    filters = {"_id": {"$in": [schedule.id for schedule in schedules]}}
                    update = {
                        "$set": {
                            "acquired_by": scheduler_id,
                            **marshal_timestamp(acquired_until, "acquired_until"),
                        }
                    }
                    self._schedules.update_many(filters, update, session=session)

        return schedules

    async def release_schedules(
        self, scheduler_id: str, schedules: list[Schedule]
    ) -> None:
        updated_schedules: list[tuple[str, datetime]] = []
        finished_schedule_ids: list[str] = []
        task_ids = {schedule.id: schedule.task_id for schedule in schedules}

        # Update schedules that have a next fire time
        requests = []
        for schedule in schedules:
            filters = {"_id": schedule.id, "acquired_by": scheduler_id}
            if schedule.next_fire_time is not None:
                try:
                    serialized_trigger = self.serializer.serialize(schedule.trigger)
                except SerializationError:
                    self._logger.exception(
                        "Error serializing schedule %r – removing from data store",
                        schedule.id,
                    )
                    requests.append(DeleteOne(filters))
                    finished_schedule_ids.append(schedule.id)
                    continue

                update = {
                    "$unset": {
                        "acquired_by": True,
                        "acquired_until": True,
                        "acquired_until_utcoffset": True,
                    },
                    "$set": {
                        "trigger": serialized_trigger,
                        **marshal_timestamp(schedule.next_fire_time, "next_fire_time"),
                    },
                }
                requests.append(UpdateOne(filters, update))
                updated_schedules.append((schedule.id, schedule.next_fire_time))
            else:
                requests.append(DeleteOne(filters))
                finished_schedule_ids.append(schedule.id)

            if requests:
                async for attempt in self._retry():
                    with attempt, self.client.start_session() as session:
                        self._schedules.bulk_write(
                            requests, ordered=False, session=session
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
                document = self._schedules.find_one(
                    {"next_fire_time": {"$ne": None}},
                    projection=["next_fire_time", "next_fire_time_utcoffset"],
                    sort=[("next_fire_time", ASCENDING)],
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
                self._jobs.insert_one(document)

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
                cursor = self._jobs.find(filters).sort("_id")
                for document in cursor:
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

    async def acquire_jobs(self, worker_id: str, limit: int | None = None) -> list[Job]:
        async for attempt in self._retry():
            with attempt, self.client.start_session() as session:
                now = datetime.now(timezone.utc)
                cursor = self._jobs.find(
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
                documents = list(cursor)

                # Retrieve the limits
                task_ids: set[str] = {document["task_id"] for document in documents}
                task_limits = self._tasks.find(
                    {"_id": {"$in": list(task_ids)}, "max_running_jobs": {"$ne": None}},
                    projection=["max_running_jobs", "running_jobs"],
                    session=session,
                )
                job_slots_left = {
                    doc["_id"]: doc["max_running_jobs"] - doc["running_jobs"]
                    for doc in task_limits
                }

                # Filter out jobs that don't have free slots
                acquired_jobs: list[Job] = []
                increments: dict[str, int] = defaultdict(lambda: 0)
                for document in documents:
                    document["id"] = document.pop("_id")
                    unmarshal_timestamps(document)
                    job = Job.unmarshal(self.serializer, document)

                    # Don't acquire the job if there are no free slots left
                    slots_left = job_slots_left.get(job.task_id)
                    if slots_left == 0:
                        continue
                    elif slots_left is not None:
                        job_slots_left[job.task_id] -= 1

                    acquired_jobs.append(job)
                    increments[job.task_id] += 1

                if acquired_jobs:
                    now = datetime.now(timezone.utc)
                    acquired_until = datetime.fromtimestamp(
                        now.timestamp() + self.lock_expiration_delay, timezone.utc
                    )
                    filters = {"_id": {"$in": [job.id for job in acquired_jobs]}}
                    update = {
                        "$set": {
                            "acquired_by": worker_id,
                            **marshal_timestamp(acquired_until, "acquired_until"),
                        }
                    }
                    self._jobs.update_many(filters, update, session=session)

                    # Increment the running job counters on each task
                    for task_id, increment in increments.items():
                        self._tasks.find_one_and_update(
                            {"_id": task_id},
                            {"$inc": {"running_jobs": increment}},
                            session=session,
                        )

        # Publish the appropriate events
        for job in acquired_jobs:
            await self._event_broker.publish(
                JobAcquired(job_id=job.id, scheduler_id=worker_id)
            )

        return acquired_jobs

    async def release_job(
        self, worker_id: str, task_id: str, result: JobResult
    ) -> None:
        async for attempt in self._retry():
            with attempt, self.client.start_session() as session:
                # Record the job result
                if result.expires_at > result.finished_at:
                    document = result.marshal(self.serializer)
                    document["_id"] = document.pop("job_id")
                    marshal_document(document)
                    self._jobs_results.insert_one(document, session=session)

                # Decrement the running jobs counter
                self._tasks.find_one_and_update(
                    {"_id": task_id}, {"$inc": {"running_jobs": -1}}, session=session
                )

                # Delete the job
                self._jobs.delete_one({"_id": result.job_id}, session=session)

    async def get_job_result(self, job_id: UUID) -> JobResult | None:
        async for attempt in self._retry():
            with attempt:
                document = self._jobs_results.find_one_and_delete({"_id": job_id})

        if document:
            document["job_id"] = document.pop("_id")
            unmarshal_timestamps(document)
            return JobResult.unmarshal(self.serializer, document)
        else:
            return None
