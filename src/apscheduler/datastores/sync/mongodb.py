from __future__ import annotations

import logging
from collections import defaultdict
from contextlib import ExitStack
from datetime import datetime, timezone
from typing import Any, Callable, ClassVar, Dict, Iterable, List, Optional, Set, Tuple, Type
from uuid import UUID

import attr
import pymongo
from pymongo import ASCENDING, DeleteOne, MongoClient, UpdateOne
from pymongo.collection import Collection
from pymongo.errors import DuplicateKeyError

from ... import events
from ...abc import DataStore, Job, Schedule, Serializer
from ...events import (
    DataStoreEvent, EventHub, JobAdded, ScheduleAdded, ScheduleRemoved, ScheduleUpdated,
    SubscriptionToken, TaskAdded, TaskRemoved)
from ...exceptions import (
    ConflictingIdError, DeserializationError, SerializationError, TaskLookupError)
from ...policies import ConflictPolicy
from ...serializers.pickle import PickleSerializer
from ...structures import JobResult, Task
from ...util import reentrant


@reentrant
class MongoDBDataStore(DataStore):
    _task_attrs: ClassVar[List[str]] = [field.name for field in attr.fields(Task)]
    _schedule_attrs: ClassVar[List[str]] = [field.name for field in attr.fields(Schedule)]
    _job_attrs: ClassVar[List[str]] = [field.name for field in attr.fields(Job)]

    def __init__(self, client: MongoClient, *, serializer: Optional[Serializer] = None,
                 database: str = 'apscheduler', tasks_collection: str = 'tasks',
                 schedules_collection: str = 'schedules', jobs_collection: str = 'jobs',
                 job_results_collection: str = 'job_results',
                 lock_expiration_delay: float = 30, start_from_scratch: bool = False):
        super().__init__()
        if not client.delegate.codec_options.tz_aware:
            raise ValueError('MongoDB client must have tz_aware set to True')

        self.client = client
        self.serializer = serializer or PickleSerializer()
        self.lock_expiration_delay = lock_expiration_delay
        self.start_from_scratch = start_from_scratch
        self._local_tasks: Dict[str, Task] = {}
        self._database = client[database]
        self._tasks: Collection = self._database[tasks_collection]
        self._schedules: Collection = self._database[schedules_collection]
        self._jobs: Collection = self._database[jobs_collection]
        self._jobs_results: Collection = self._database[job_results_collection]
        self._logger = logging.getLogger(__name__)
        self._exit_stack = ExitStack()
        self._events = EventHub()

    @classmethod
    def from_url(cls, uri: str, **options) -> 'MongoDBDataStore':
        client = MongoClient(uri)
        return cls(client, **options)

    def __enter__(self):
        server_info = self.client.server_info()
        if server_info['versionArray'] < [4, 0]:
            raise RuntimeError(f"MongoDB server must be at least v4.0; current version = "
                               f"{server_info['version']}")

        self._exit_stack.__enter__()
        self._exit_stack.enter_context(self._events)

        if self.start_from_scratch:
            self._tasks.delete_many({})
            self._schedules.delete_many({})
            self._jobs.delete_many({})
            self._jobs_results.delete_many({})

        self._schedules.create_index('next_fire_time')
        self._jobs.create_index('task_id')
        self._jobs.create_index('created_at')
        self._jobs.create_index('tags')
        self._jobs_results.create_index('finished_at')
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._exit_stack.__exit__(exc_type, exc_val, exc_tb)

    def subscribe(self, callback: Callable[[events.Event], Any],
                  event_types: Optional[Iterable[Type[events.Event]]] = None) -> SubscriptionToken:
        return self._events.subscribe(callback, event_types)

    def unsubscribe(self, token: events.SubscriptionToken) -> None:
        self._events.unsubscribe(token)

    def add_task(self, task: Task) -> None:
        self._tasks.find_one_and_update(
            {'_id': task.id},
            {'$set': task.marshal(self.serializer),
             '$setOnInsert': {'running_jobs': 0}},
            upsert=True
        )
        self._local_tasks[task.id] = task
        self._events.publish(TaskAdded(task_id=task.id))

    def remove_task(self, task_id: str) -> None:
        if not self._tasks.find_one_and_delete({'_id': task_id}):
            raise TaskLookupError(task_id)

        del self._local_tasks[task_id]
        self._events.publish(TaskRemoved(task_id=task_id))

    def get_task(self, task_id: str) -> Task:
        try:
            return self._local_tasks[task_id]
        except KeyError:
            document = self._tasks.find_one({'_id': task_id}, projection=self._task_attrs)
            if not document:
                raise TaskLookupError(task_id)

            document['id'] = document.pop('id')
            task = self._local_tasks[task_id] = Task.unmarshal(self.serializer, document)
            return task

    def get_tasks(self) -> List[Task]:
        tasks: List[Task] = []
        for document in self._tasks.find(projection=self._task_attrs,
                                         sort=[('_id', pymongo.ASCENDING)]):
            document['id'] = document.pop('_id')
            tasks.append(Task.unmarshal(self.serializer, document))

        return tasks

    def get_schedules(self, ids: Optional[Set[str]] = None) -> List[Schedule]:
        schedules: List[Schedule] = []
        filters = {'_id': {'$in': list(ids)}} if ids is not None else {}
        cursor = self._schedules.find(filters, projection=['_id', 'serialized_data']).sort('_id')
        for document in cursor:
            try:
                schedule = self.serializer.deserialize(document['serialized_data'])
            except DeserializationError:
                self._logger.warning('Failed to deserialize schedule %r', document['_id'])
                continue

            schedules.append(schedule)

        return schedules

    def add_schedule(self, schedule: Schedule, conflict_policy: ConflictPolicy) -> None:
        event: DataStoreEvent
        serialized_data = self.serializer.serialize(schedule)
        document = {
            '_id': schedule.id,
            'task_id': schedule.task_id,
            'serialized_data': serialized_data,
            'next_fire_time': schedule.next_fire_time
        }
        try:
            self._schedules.insert_one(document)
        except DuplicateKeyError:
            if conflict_policy is ConflictPolicy.exception:
                raise ConflictingIdError(schedule.id) from None
            elif conflict_policy is ConflictPolicy.replace:
                self._schedules.replace_one({'_id': schedule.id}, document, True)
                event = ScheduleUpdated(
                    schedule_id=schedule.id,
                    next_fire_time=schedule.next_fire_time)
                self._events.publish(event)
        else:
            event = ScheduleAdded(schedule_id=schedule.id,
                                  next_fire_time=schedule.next_fire_time)
            self._events.publish(event)

    def remove_schedules(self, ids: Iterable[str]) -> None:
        with self.client.start_session() as s, s.start_transaction():
            filters = {'_id': {'$in': list(ids)}} if ids is not None else {}
            cursor = self._schedules.find(filters, projection=['_id'])
            ids = [doc['_id'] for doc in cursor]
            if ids:
                self._schedules.delete_many(filters)

        for schedule_id in ids:
            self._events.publish(ScheduleRemoved(schedule_id=schedule_id))

    def acquire_schedules(self, scheduler_id: str, limit: int) -> List[Schedule]:
        schedules: List[Schedule] = []
        with self.client.start_session() as s, s.start_transaction():
            cursor = self._schedules.find(
                {'next_fire_time': {'$ne': None},
                 '$or': [{'acquired_until': {'$exists': False}},
                         {'acquired_until': {'$lt': datetime.now(timezone.utc)}}]
                 },
                projection=['serialized_data']
            ).sort('next_fire_time').limit(limit)
            for document in cursor:
                schedule = self.serializer.deserialize(document['serialized_data'])
                schedules.append(schedule)

            if schedules:
                now = datetime.now(timezone.utc)
                acquired_until = datetime.fromtimestamp(
                    now.timestamp() + self.lock_expiration_delay, now.tzinfo)
                filters = {'_id': {'$in': [schedule.id for schedule in schedules]}}
                update = {'$set': {'acquired_by': scheduler_id,
                                   'acquired_until': acquired_until}}
                self._schedules.update_many(filters, update)

        return schedules

    def release_schedules(self, scheduler_id: str, schedules: List[Schedule]) -> None:
        updated_schedules: List[Tuple[str, datetime]] = []
        finished_schedule_ids: List[str] = []
        with self.client.start_session() as s, s.start_transaction():
            # Update schedules that have a next fire time
            requests = []
            for schedule in schedules:
                filters = {'_id': schedule.id, 'acquired_by': scheduler_id}
                if schedule.next_fire_time is not None:
                    try:
                        serialized_data = self.serializer.serialize(schedule)
                    except SerializationError:
                        self._logger.exception('Error serializing schedule %r – '
                                               'removing from data store', schedule.id)
                        requests.append(DeleteOne(filters))
                        finished_schedule_ids.append(schedule.id)
                        continue

                    update = {
                        '$unset': {
                            'acquired_by': True,
                            'acquired_until': True,
                        },
                        '$set': {
                            'next_fire_time': schedule.next_fire_time,
                            'serialized_data': serialized_data
                        }
                    }
                    requests.append(UpdateOne(filters, update))
                    updated_schedules.append((schedule.id, schedule.next_fire_time))
                else:
                    requests.append(DeleteOne(filters))
                    finished_schedule_ids.append(schedule.id)

            if requests:
                self._schedules.bulk_write(requests, ordered=False)
                for schedule_id, next_fire_time in updated_schedules:
                    event = ScheduleUpdated(schedule_id=schedule_id, next_fire_time=next_fire_time)
                    self._events.publish(event)

        for schedule_id in finished_schedule_ids:
            self._events.publish(ScheduleRemoved(schedule_id=schedule_id))

    def get_next_schedule_run_time(self) -> Optional[datetime]:
        document = self._schedules.find_one({'next_run_time': {'$ne': None}},
                                            projection=['next_run_time'],
                                            sort=[('next_run_time', ASCENDING)])
        if document:
            return document['next_run_time']
        else:
            return None

    def add_job(self, job: Job) -> None:
        serialized_data = self.serializer.serialize(job)
        document = {
            '_id': job.id,
            'serialized_data': serialized_data,
            'task_id': job.task_id,
            'created_at': datetime.now(timezone.utc),
            'tags': list(job.tags)
        }
        self._jobs.insert_one(document)
        event = JobAdded(job_id=job.id, task_id=job.task_id, schedule_id=job.schedule_id,
                         tags=job.tags)
        self._events.publish(event)

    def get_jobs(self, ids: Optional[Iterable[UUID]] = None) -> List[Job]:
        jobs: List[Job] = []
        filters = {'_id': {'$in': list(ids)}} if ids is not None else {}
        cursor = self._jobs.find(filters, projection=['_id', 'serialized_data']).sort('_id')
        for document in cursor:
            try:
                job = self.serializer.deserialize(document['serialized_data'])
            except DeserializationError:
                self._logger.warning('Failed to deserialize job %r', document['_id'])
                continue

            jobs.append(job)

        return jobs

    def acquire_jobs(self, worker_id: str, limit: Optional[int] = None) -> List[Job]:
        with self.client.start_session() as session:
            cursor = self._jobs.find(
                {'$or': [{'acquired_until': {'$exists': False}},
                         {'acquired_until': {'$lt': datetime.now(timezone.utc)}}]
                 },
                projection=['task_id', 'serialized_data'],
                sort=[('created_at', ASCENDING)],
                limit=limit,
                session=session
            )
            documents = list(cursor)

            # Retrieve the limits
            task_ids: Set[str] = {document['task_id'] for document in documents}
            task_limits = self._tasks.find(
                {'_id': {'$in': list(task_ids)}, 'max_running_jobs': {'$ne': None}},
                projection=['max_running_jobs', 'running_jobs'],
                session=session
            )
            job_slots_left = {doc['_id']: doc['max_running_jobs'] - doc['running_jobs']
                              for doc in task_limits}

            # Filter out jobs that don't have free slots
            acquired_jobs: List[Job] = []
            increments: Dict[str, int] = defaultdict(lambda: 0)
            for document in documents:
                job = self.serializer.deserialize(document['serialized_data'])

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
                    now.timestamp() + self.lock_expiration_delay, timezone.utc)
                filters = {'_id': {'$in': [job.id for job in acquired_jobs]}}
                update = {'$set': {'acquired_by': worker_id,
                                   'acquired_until': acquired_until}}
                self._jobs.update_many(filters, update, session=session)

                # Increment the running job counters on each task
                for task_id, increment in increments.items():
                    self._tasks.find_one_and_update(
                        {'_id': task_id},
                        {'$inc': {'running_jobs': increment}},
                        session=session
                    )

            return acquired_jobs

    def release_job(self, worker_id: str, job: Job, result: Optional[JobResult]) -> None:
        with self.client.start_session() as session:
            # Insert the job result
            now = datetime.now(timezone.utc)
            document = {
                '_id': job.id,
                'finished_at': now,
                'serialized_data': self.serializer.serialize(result)
            }
            self._jobs_results.insert_one(document, session=session)

            # Decrement the running jobs counter
            self._tasks.find_one_and_update(
                {'_id': job.task_id},
                {'$inc': {'running_jobs': -1}}
            )

            # Delete the job
            self._jobs.delete_one({'_id': job.id}, session=session)

    def get_job_result(self, job_id: UUID) -> Optional[JobResult]:
        document = self._jobs_results.find_one_and_delete(
            filter={'_id': job_id}, projection=['serialized_data'])
        return self.serializer.deserialize(document['serialized_data']) if document else None
