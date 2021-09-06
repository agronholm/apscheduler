from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone
from logging import Logger, getLogger
from typing import Any, Callable, Iterable, Optional, Tuple, Type
from uuid import UUID

import attr
from sqlalchemy import (
    TIMESTAMP, Column, Integer, LargeBinary, MetaData, Table, TypeDecorator, Unicode, and_,
    bindparam, or_, select)
from sqlalchemy.engine import URL, Dialect
from sqlalchemy.exc import CompileError, IntegrityError
from sqlalchemy.future import Engine, create_engine
from sqlalchemy.sql.ddl import DropTable
from sqlalchemy.sql.elements import BindParameter, literal

from ...abc import DataStore, Job, Schedule, Serializer
from ...enums import ConflictPolicy
from ...events import (
    Event, EventHub, JobAdded, JobDeserializationFailed, ScheduleAdded,
    ScheduleDeserializationFailed, ScheduleRemoved, ScheduleUpdated, SubscriptionToken, TaskAdded,
    TaskRemoved, TaskUpdated)
from ...exceptions import ConflictingIdError, SerializationError, TaskLookupError
from ...marshalling import callable_to_ref
from ...serializers.pickle import PickleSerializer
from ...structures import JobResult, Task
from ...util import reentrant


class EmulatedUUID(TypeDecorator):
    impl = Unicode(32)
    cache_ok = True

    def process_bind_param(self, value, dialect: Dialect) -> Any:
        return value.hex

    def process_result_value(self, value: Any, dialect: Dialect):
        return UUID(value) if value else None


@reentrant
@attr.define(eq=False)
class SQLAlchemyDataStore(DataStore):
    engine: Engine
    schema: Optional[str] = attr.field(default=None, kw_only=True)
    serializer: Serializer = attr.field(factory=PickleSerializer, kw_only=True)
    lock_expiration_delay: float = attr.field(default=30, kw_only=True)
    max_poll_time: Optional[float] = attr.field(default=1, kw_only=True)
    max_idle_time: float = attr.field(default=60, kw_only=True)
    notify_channel: Optional[str] = attr.field(default='apscheduler', kw_only=True)
    start_from_scratch: bool = attr.field(default=False, kw_only=True)

    _logger: Logger = attr.field(init=False, factory=lambda: getLogger(__name__))
    _events: EventHub = attr.field(init=False, factory=EventHub)

    def __attrs_post_init__(self) -> None:
        # Generate the table definitions
        self._metadata = self.get_table_definitions()
        self.t_metadata = self._metadata.tables['metadata']
        self.t_tasks = self._metadata.tables['tasks']
        self.t_schedules = self._metadata.tables['schedules']
        self.t_jobs = self._metadata.tables['jobs']
        self.t_job_results = self._metadata.tables['job_results']

        # Find out if the dialect supports RETURNING
        update = self.t_jobs.update().returning(self.t_jobs.c.id)
        try:
            update.compile(bind=self.engine)
        except CompileError:
            self._supports_update_returning = False
        else:
            self._supports_update_returning = True

    @classmethod
    def from_url(cls, url: str | URL, **options) -> 'SQLAlchemyDataStore':
        engine = create_engine(url)
        return cls(engine, **options)

    def __enter__(self):
        with self.engine.begin() as conn:
            if self.start_from_scratch:
                for table in self._metadata.sorted_tables:
                    conn.execute(DropTable(table, if_exists=True))

            self._metadata.create_all(conn)
            query = select(self.t_metadata.c.schema_version)
            result = conn.execute(query)
            version = result.scalar()
            if version is None:
                conn.execute(self.t_metadata.insert(values={'schema_version': 1}))
            elif version > 1:
                raise RuntimeError(f'Unexpected schema version ({version}); '
                                   f'only version 1 is supported by this version of APScheduler')

        self._events.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._events.__exit__(exc_type, exc_val, exc_tb)

    def get_table_definitions(self) -> MetaData:
        if self.engine.dialect.name in ('mysql', 'mariadb'):
            from sqlalchemy.dialects import mysql
            timestamp_type = mysql.TIMESTAMP(fsp=6)
        else:
            timestamp_type = TIMESTAMP(timezone=True)

        if self.engine.dialect.name == 'postgresql':
            from sqlalchemy.dialects import postgresql

            job_id_type = postgresql.UUID(as_uuid=True)
        else:
            job_id_type = EmulatedUUID

        metadata = MetaData()
        Table(
            'metadata',
            metadata,
            Column('schema_version', Integer, nullable=False)
        )
        Table(
            'tasks',
            metadata,
            Column('id', Unicode(500), primary_key=True),
            Column('func', Unicode(500), nullable=False),
            Column('state', LargeBinary),
            Column('max_running_jobs', Integer),
            Column('misfire_grace_time', Unicode(16)),
            Column('running_jobs', Integer, nullable=False, server_default=literal(0))
        )
        Table(
            'schedules',
            metadata,
            Column('id', Unicode(500), primary_key=True),
            Column('task_id', Unicode(500), nullable=False, index=True),
            Column('serialized_data', LargeBinary, nullable=False),
            Column('next_fire_time', timestamp_type, index=True),
            Column('acquired_by', Unicode(500)),
            Column('acquired_until', timestamp_type)
        )
        Table(
            'jobs',
            metadata,
            Column('id', job_id_type, primary_key=True),
            Column('task_id', Unicode(500), nullable=False, index=True),
            Column('serialized_data', LargeBinary, nullable=False),
            Column('created_at', timestamp_type, nullable=False),
            Column('acquired_by', Unicode(500)),
            Column('acquired_until', timestamp_type)
        )
        Table(
            'job_results',
            metadata,
            Column('job_id', job_id_type, primary_key=True),
            Column('finished_at', timestamp_type, index=True),
            Column('serialized_data', LargeBinary, nullable=False)
        )
        return metadata

    def _deserialize_jobs(self, serialized_jobs: Iterable[Tuple[UUID, bytes]]) -> list[Job]:
        jobs: list[Job] = []
        for job_id, serialized_data in serialized_jobs:
            try:
                jobs.append(self.serializer.deserialize(serialized_data))
            except SerializationError as exc:
                self._events.publish(JobDeserializationFailed(job_id=job_id, exception=exc))

        return jobs

    def _deserialize_schedules(
            self, serialized_schedules: Iterable[Tuple[str, bytes]]) -> list[Schedule]:
        jobs: list[Schedule] = []
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

    def add_task(self, task: Task) -> None:
        insert = self.t_tasks.insert().\
            values(id=task.id, func=callable_to_ref(task.func),
                   max_running_jobs=task.max_running_jobs,
                   misfire_grace_time=task.misfire_grace_time)
        try:
            with self.engine.begin() as conn:
                conn.execute(insert)
        except IntegrityError:
            update = self.t_tasks.update().\
                values(func=callable_to_ref(task.func), max_running_jobs=task.max_running_jobs,
                       misfire_grace_time=task.misfire_grace_time).\
                where(self.t_tasks.c.id == task.id)
            with self.engine.begin() as conn:
                conn.execute(update)
                self._events.publish(TaskUpdated(task_id=task.id))
        else:
            self._events.publish(TaskAdded(task_id=task.id))

    def remove_task(self, task_id: str) -> None:
        delete = self.t_tasks.delete().where(self.t_tasks.c.id == task_id)
        with self.engine.begin() as conn:
            result = conn.execute(delete)
            if result.rowcount == 0:
                raise TaskLookupError(task_id)
            else:
                self._events.publish(TaskRemoved(task_id=task_id))

    def get_task(self, task_id: str) -> Task:
        query = select([self.t_tasks.c.id, self.t_tasks.c.func, self.t_tasks.c.max_running_jobs,
                        self.t_tasks.c.state, self.t_tasks.c.misfire_grace_time]).\
            where(self.t_tasks.c.id == task_id)
        with self.engine.begin() as conn:
            result = conn.execute(query)
            row = result.fetch_one()

        if row:
            return Task.unmarshal(self.serializer, row._asdict())
        else:
            raise TaskLookupError

    def get_tasks(self) -> list[Task]:
        query = select([self.t_tasks.c.id, self.t_tasks.c.func, self.t_tasks.c.max_running_jobs,
                        self.t_tasks.c.state, self.t_tasks.c.misfire_grace_time]).\
            order_by(self.t_tasks.c.id)
        with self.engine.begin() as conn:
            result = conn.execute(query)
            tasks = [Task.unmarshal(self.serializer, row._asdict()) for row in result]
            return tasks

    def add_schedule(self, schedule: Schedule, conflict_policy: ConflictPolicy) -> None:
        event: Event
        serialized_data = self.serializer.serialize(schedule)
        insert = self.t_schedules.insert().\
            values(id=schedule.id, task_id=schedule.task_id, serialized_data=serialized_data,
                   next_fire_time=schedule.next_fire_time)
        try:
            with self.engine.begin() as conn:
                conn.execute(insert)
                event = ScheduleAdded(schedule_id=schedule.id,
                                      next_fire_time=schedule.next_fire_time)
                self._events.publish(event)
        except IntegrityError:
            if conflict_policy is ConflictPolicy.exception:
                raise ConflictingIdError(schedule.id) from None
            elif conflict_policy is ConflictPolicy.replace:
                update = self.t_schedules.update().\
                    where(self.t_schedules.c.id == schedule.id).\
                    values(serialized_data=serialized_data,
                           next_fire_time=schedule.next_fire_time)
                with self.engine.begin() as conn:
                    conn.execute(update)

                event = ScheduleUpdated(schedule_id=schedule.id,
                                        next_fire_time=schedule.next_fire_time)
                self._events.publish(event)

    def remove_schedules(self, ids: Iterable[str]) -> None:
        with self.engine.begin() as conn:
            delete = self.t_schedules.delete().where(self.t_schedules.c.id.in_(ids))
            if self._supports_update_returning:
                delete = delete.returning(self.t_schedules.c.id)
                removed_ids: Iterable[str] = [row[0] for row in conn.execute(delete)]
            else:
                # TODO: actually check which rows were deleted?
                conn.execute(delete)
                removed_ids = ids

        for schedule_id in removed_ids:
            self._events.publish(ScheduleRemoved(schedule_id=schedule_id))

    def get_schedules(self, ids: Optional[set[str]] = None) -> list[Schedule]:
        query = select([self.t_schedules.c.id, self.t_schedules.c.serialized_data]).\
            order_by(self.t_schedules.c.id)
        if ids:
            query = query.where(self.t_schedules.c.id.in_(ids))

        with self.engine.begin() as conn:
            result = conn.execute(query)
            return self._deserialize_schedules(result)

    def acquire_schedules(self, scheduler_id: str, limit: int) -> list[Schedule]:
        with self.engine.begin() as conn:
            now = datetime.now(timezone.utc)
            acquired_until = now + timedelta(seconds=self.lock_expiration_delay)
            schedules_cte = select(self.t_schedules.c.id).\
                where(and_(self.t_schedules.c.next_fire_time.isnot(None),
                           self.t_schedules.c.next_fire_time <= now,
                           or_(self.t_schedules.c.acquired_until.is_(None),
                               self.t_schedules.c.acquired_until < now))).\
                order_by(self.t_schedules.c.next_fire_time).\
                limit(limit).cte()
            subselect = select([schedules_cte.c.id])
            update = self.t_schedules.update().\
                where(self.t_schedules.c.id.in_(subselect)).\
                values(acquired_by=scheduler_id, acquired_until=acquired_until)
            if self._supports_update_returning:
                update = update.returning(self.t_schedules.c.id,
                                          self.t_schedules.c.serialized_data)
                result = conn.execute(update)
            else:
                conn.execute(update)
                query = select([self.t_schedules.c.id, self.t_schedules.c.serialized_data]).\
                    where(and_(self.t_schedules.c.acquired_by == scheduler_id))
                result = conn.execute(query)

            schedules = self._deserialize_schedules(result)

        return schedules

    def release_schedules(self, scheduler_id: str, schedules: list[Schedule]) -> None:
        with self.engine.begin() as conn:
            update_events: list[ScheduleUpdated] = []
            finished_schedule_ids: list[str] = []
            update_args: list[dict[str, Any]] = []
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
                p_id: BindParameter = bindparam('p_id')
                p_serialized: BindParameter = bindparam('p_serialized_data')
                p_next_fire_time: BindParameter = bindparam('p_next_fire_time')
                update = self.t_schedules.update().\
                    where(and_(self.t_schedules.c.id == p_id,
                               self.t_schedules.c.acquired_by == scheduler_id)).\
                    values(serialized_data=p_serialized, next_fire_time=p_next_fire_time,
                           acquired_by=None, acquired_until=None)
                next_fire_times = {arg['p_id']: arg['p_next_fire_time'] for arg in update_args}
                if self._supports_update_returning:
                    update = update.returning(self.t_schedules.c.id)
                    updated_ids = [row[0] for row in conn.execute(update, update_args)]
                else:
                    # TODO: actually check which rows were updated?
                    conn.execute(update, update_args)
                    updated_ids = list(next_fire_times)

                for schedule_id in updated_ids:
                    event = ScheduleUpdated(schedule_id=schedule_id,
                                            next_fire_time=next_fire_times[schedule_id])
                    update_events.append(event)

            # Remove schedules that have no next fire time or failed to serialize
            if finished_schedule_ids:
                delete = self.t_schedules.delete().\
                    where(self.t_schedules.c.id.in_(finished_schedule_ids))
                conn.execute(delete)

        for event in update_events:
            self._events.publish(event)

        for schedule_id in finished_schedule_ids:
            self._events.publish(ScheduleRemoved(schedule_id=schedule_id))

    def get_next_schedule_run_time(self) -> Optional[datetime]:
        query = select(self.t_schedules.c.id).\
            where(self.t_schedules.c.next_fire_time.isnot(None)).\
            order_by(self.t_schedules.c.next_fire_time).\
            limit(1)
        with self.engine.begin() as conn:
            result = conn.execute(query)
            return result.scalar()

    def add_job(self, job: Job) -> None:
        now = datetime.now(timezone.utc)
        serialized_data = self.serializer.serialize(job)
        insert = self.t_jobs.insert().values(id=job.id, task_id=job.task_id,
                                             created_at=now, serialized_data=serialized_data)
        with self.engine.begin() as conn:
            conn.execute(insert)

        event = JobAdded(job_id=job.id, task_id=job.task_id, schedule_id=job.schedule_id,
                         tags=job.tags)
        self._events.publish(event)

    def get_jobs(self, ids: Optional[Iterable[UUID]] = None) -> list[Job]:
        query = select([self.t_jobs.c.id, self.t_jobs.c.serialized_data]).\
            order_by(self.t_jobs.c.id)
        if ids:
            job_ids = [job_id for job_id in ids]
            query = query.where(self.t_jobs.c.id.in_(job_ids))

        with self.engine.begin() as conn:
            result = conn.execute(query)
            return self._deserialize_jobs(result)

    def acquire_jobs(self, worker_id: str, limit: Optional[int] = None) -> list[Job]:
        with self.engine.begin() as conn:
            now = datetime.now(timezone.utc)
            acquired_until = now + timedelta(seconds=self.lock_expiration_delay)
            query = select([self.t_jobs.c.id, self.t_jobs.c.serialized_data]).\
                join(self.t_tasks, self.t_tasks.c.id == self.t_jobs.c.task_id).\
                where(or_(self.t_jobs.c.acquired_until.is_(None),
                          self.t_jobs.c.acquired_until < now)).\
                order_by(self.t_jobs.c.created_at).\
                limit(limit)

            result = conn.execute(query)
            if not result:
                return []

            # Mark the jobs as acquired by this worker
            jobs = self._deserialize_jobs(result)
            task_ids: set[str] = {job.task_id for job in jobs}

            # Retrieve the limits
            query = select([self.t_tasks.c.id,
                            self.t_tasks.c.max_running_jobs - self.t_tasks.c.running_jobs]).\
                where(self.t_tasks.c.max_running_jobs.isnot(None),
                      self.t_tasks.c.id.in_(task_ids))
            result = conn.execute(query)
            job_slots_left = dict(result.fetchall())

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
                update = self.t_jobs.update().\
                    values(acquired_by=worker_id, acquired_until=acquired_until).\
                    where(self.t_jobs.c.id.in_(acquired_job_ids))
                conn.execute(update)

                # Increment the running job counters on each task
                p_id: BindParameter = bindparam('p_id')
                p_increment: BindParameter = bindparam('p_increment')
                params = [{'p_id': task_id, 'p_increment': increment}
                          for task_id, increment in increments.items()]
                update = self.t_tasks.update().\
                    values(running_jobs=self.t_tasks.c.running_jobs + p_increment).\
                    where(self.t_tasks.c.id == p_id)
                conn.execute(update, params)

            return acquired_jobs

    def release_job(self, worker_id: str, job: Job, result: Optional[JobResult]) -> None:
        with self.engine.begin() as conn:
            # Insert the job result
            now = datetime.now(timezone.utc)
            serialized_result = self.serializer.serialize(result)
            insert = self.t_job_results.insert().\
                values(job_id=job.id, finished_at=now, serialized_data=serialized_result)
            conn.execute(insert)

            # Decrement the running jobs counter
            update = self.t_tasks.update().\
                values(running_jobs=self.t_tasks.c.running_jobs - 1).\
                where(self.t_tasks.c.id == job.task_id)
            conn.execute(update)

            # Delete the job
            delete = self.t_jobs.delete().where(self.t_jobs.c.id == job.id)
            conn.execute(delete)

    def get_job_result(self, job_id: UUID) -> Optional[JobResult]:
        with self.engine.begin() as conn:
            # Retrieve the result
            query = select(self.t_job_results.c.serialized_data).\
                where(self.t_job_results.c.job_id == job_id)
            result = conn.execute(query)

            # Delete the result
            delete = self.t_job_results.delete().\
                where(self.t_job_results.c.job_id == job_id)
            conn.execute(delete)

            serialized_result = result.scalar()
            return self.serializer.deserialize(serialized_result) if serialized_result else None
