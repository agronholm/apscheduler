import asyncio
import logging
from datetime import datetime, timezone
from typing import Iterable, List, Optional, Set, Tuple

import sniffio
from anyio import create_task_group, move_on_after
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection
from pymongo import ASCENDING, DeleteOne, UpdateOne
from pymongo.collection import Collection
from pymongo.errors import DuplicateKeyError

from ..abc import DataStore, Job, Schedule, Serializer
from ..events import EventHub, ScheduleAdded, ScheduleRemoved, ScheduleUpdated
from ..exceptions import ConflictingIdError, DeserializationError, SerializationError
from ..policies import ConflictPolicy
from ..serializers.pickle import PickleSerializer


class MongoDBDataStore(DataStore, EventHub):
    def __init__(self, client: AsyncIOMotorClient, *, serializer: Optional[Serializer] = None,
                 database: str = 'apscheduler', schedules_collection: str = 'schedules',
                 jobs_collection: str = 'jobs', lock_expiration_delay: float = 30,
                 max_poll_time: Optional[float] = 1, start_from_scratch: bool = False):
        super().__init__()
        if not client.delegate.codec_options.tz_aware:
            raise ValueError('MongoDB client must have tz_aware set to True')

        self.client = client
        self.serializer = serializer or PickleSerializer()
        self.lock_expiration_delay = lock_expiration_delay
        self.max_poll_time = max_poll_time
        self.start_from_scratch = start_from_scratch
        self._database = client[database]
        self._schedules: AsyncIOMotorCollection = self._database[schedules_collection]
        self._jobs: AsyncIOMotorCollection = self._database[jobs_collection]
        self._logger = logging.getLogger(__name__)
        self._watch_collections = hasattr(self.client, '_topology_settings')
        self._loans = 0

    async def __aenter__(self):
        asynclib = sniffio.current_async_library() or '(unknown)'
        if asynclib != 'asyncio':
            raise RuntimeError(f'This data store requires asyncio; currently running: {asynclib}')

        if self._loans == 0:
            self._schedules_event = asyncio.Event()
            self._jobs_event = asyncio.Event()
            await self._setup()

        self._loans += 1
        if self._loans == 1 and self._watch_collections:
            self._task_group = create_task_group()
            await self._task_group.__aenter__()
            self._task_group.start_soon(self._watch_collection, self._schedules)
            self._task_group.start_soon(self._watch_collection, self._jobs)

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        assert self._loans
        self._loans -= 1
        if self._loans == 0 and self._watch_collections:
            self._task_group.cancel_scope.cancel()
            await self._task_group.__aexit__(exc_type, exc_val, exc_tb)

    async def _watch_collection(self, collection: Collection) -> None:
        pipeline = [{'$match': {'operationType': {'$in': ['insert', 'update', 'replace']}}},
                    {'$project': ['next_fire_time']}]
        async with collection.watch(pipeline, batch_size=1,
                                    max_await_time_ms=self.max_poll_time) as stream:
            await stream.next()

    async def _setup(self) -> None:
        server_info = await self.client.server_info()
        if server_info['versionArray'] < [4, 0]:
            raise RuntimeError(f"MongoDB server must be at least v4.0; current version = "
                               f"{server_info['version']}")

        if self.start_from_scratch:
            await self._schedules.delete_many({})
            await self._jobs.delete_many({})

        await self._schedules.create_index('next_fire_time')
        await self._jobs.create_index('task_id')
        await self._jobs.create_index('created_at')
        await self._jobs.create_index('tags')

    async def get_schedules(self, ids: Optional[Set[str]] = None) -> List[Schedule]:
        schedules: List[Schedule] = []
        filters = {'_id': {'$in': list(ids)}} if ids is not None else {}
        cursor = self._schedules.find(filters, projection=['_id', 'serialized_data']).sort('_id')
        for document in await cursor.to_list(None):
            try:
                schedule = self.serializer.deserialize(document['serialized_data'])
            except DeserializationError:
                self._logger.warning('Failed to deserialize schedule %r', document['_id'])
                continue

            schedules.append(schedule)

        return schedules

    async def add_schedule(self, schedule: Schedule, conflict_policy: ConflictPolicy) -> None:
        serialized_data = self.serializer.serialize(schedule)
        document = {
            '_id': schedule.id,
            'task_id': schedule.task_id,
            'serialized_data': serialized_data,
            'next_fire_time': schedule.next_fire_time
        }
        try:
            await self._schedules.insert_one(document)
        except DuplicateKeyError:
            if conflict_policy is ConflictPolicy.exception:
                raise ConflictingIdError(schedule.id) from None
            elif conflict_policy is ConflictPolicy.replace:
                await self._schedules.replace_one({'_id': schedule.id}, document, True)
                await self.publish(
                    ScheduleUpdated(datetime.now(timezone.utc), schedule.id,
                                    schedule.next_fire_time)
                )
        else:
            await self.publish(
                ScheduleAdded(datetime.now(timezone.utc), schedule.id, schedule.next_fire_time)
            )

        self._schedules_event.set()

    async def remove_schedules(self, ids: Iterable[str]) -> None:
        async with await self.client.start_session() as s, s.start_transaction():
            filters = {'_id': {'$in': list(ids)}} if ids is not None else {}
            cursor = self._schedules.find(filters, projection=['_id'])
            ids = [doc['_id'] for doc in await cursor.to_list(None)]
            if ids:
                await self._schedules.delete_many(filters)

        now = datetime.now(timezone.utc)
        for schedule_id in ids:
            await self.publish(ScheduleRemoved(now, schedule_id))

    async def acquire_schedules(self, scheduler_id: str, limit: int) -> List[Schedule]:
        while True:
            schedules: List[Schedule] = []
            async with await self.client.start_session() as s, s.start_transaction():
                cursor = self._schedules.find(
                    {'next_fire_time': {'$ne': None},
                     '$or': [{'acquired_until': {'$exists': False}},
                             {'acquired_until': {'$lt': datetime.now(timezone.utc)}}]
                     },
                    projection=['serialized_data']
                ).sort('next_fire_time')
                for document in await cursor.to_list(length=limit):
                    schedule = self.serializer.deserialize(document['serialized_data'])
                    schedules.append(schedule)

                if schedules:
                    now = datetime.now(timezone.utc)
                    acquired_until = datetime.fromtimestamp(
                        now.timestamp() + self.lock_expiration_delay, now.tzinfo)
                    filters = {'_id': {'$in': [schedule.id for schedule in schedules]}}
                    update = {'$set': {'acquired_by': scheduler_id,
                                       'acquired_until': acquired_until}}
                    await self._schedules.update_many(filters, update)
                    return schedules

            async with move_on_after(self.max_poll_time):
                await self._schedules_event.wait()

    async def release_schedules(self, scheduler_id: str, schedules: List[Schedule]) -> None:
        updated_schedules: List[Tuple[str, datetime]] = []
        finished_schedule_ids: List[str] = []
        async with await self.client.start_session() as s, s.start_transaction():
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
                await self._schedules.bulk_write(requests, ordered=False)
                now = datetime.now(timezone.utc)
                for schedule_id, next_fire_time in updated_schedules:
                    await self.publish(ScheduleUpdated(now, schedule_id, next_fire_time))

        now = datetime.now(timezone.utc)
        for schedule_id in finished_schedule_ids:
            await self.publish(ScheduleRemoved(now, schedule_id))

    async def add_job(self, job: Job) -> None:
        serialized_data = self.serializer.serialize(job)
        document = {
            '_id': job.id,
            'serialized_data': serialized_data,
            'task_id': job.task_id,
            'created_at': datetime.now(timezone.utc),
            'tags': list(job.tags)
        }
        await self._jobs.insert_one(document)
        self._jobs_event.set()

    async def get_jobs(self, ids: Optional[Iterable[str]] = None) -> List[Job]:
        jobs: List[Job] = []
        filters = {'_id': {'$in': list(ids)}} if ids is not None else {}
        cursor = self._jobs.find(filters, projection=['_id', 'serialized_data']).sort('_id')
        for document in await cursor.to_list(None):
            try:
                job = self.serializer.deserialize(document['serialized_data'])
            except DeserializationError:
                self._logger.warning('Failed to deserialize job %r', document['_id'])
                continue

            jobs.append(job)

        return jobs

    async def acquire_jobs(self, worker_id: str, limit: Optional[int] = None) -> List[Job]:
        jobs: List[Job] = []
        while True:
            async with await self.client.start_session() as s, s.start_transaction():
                cursor = self._jobs.find(
                    {'$or': [{'acquired_until': {'$exists': False}},
                             {'acquired_until': {'$lt': datetime.now(timezone.utc)}}]
                     },
                    projection=['serialized_data'],
                    sort=[('created_at', ASCENDING)]
                )
                for document in await cursor.to_list(length=limit):
                    job = self.serializer.deserialize(document['serialized_data'])
                    jobs.append(job)

                if jobs:
                    now = datetime.now(timezone.utc)
                    acquired_until = datetime.fromtimestamp(
                        now.timestamp() + self.lock_expiration_delay, timezone.utc)
                    filters = {'_id': {'$in': [job.id for job in jobs]}}
                    update = {'$set': {'acquired_by': worker_id,
                                       'acquired_until': acquired_until}}
                    await self._jobs.update_many(filters, update)
                    return jobs

            async with move_on_after(self.max_poll_time):
                await self._jobs_event.wait()
                self._jobs_event.clear()

    async def release_jobs(self, worker_id: str, jobs: List[Job]) -> None:
        filters = {'_id': {'$in': [job.id for job in jobs]}, 'acquired_by': worker_id}
        await self._jobs.delete_many(filters)
