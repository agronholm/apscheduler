from __future__ import annotations

import json
import os
import sys
import threading
import time
from collections import defaultdict
from collections.abc import Callable
from contextlib import AsyncExitStack
from datetime import UTC, datetime, timedelta
from functools import partial
from inspect import signature
from pathlib import Path
from queue import Queue
from types import ModuleType
from typing import Any, cast
from unittest.mock import patch
from zoneinfo import ZoneInfo

import anyio
import attrs
import pytest
from anyio import (
    Lock,
    WouldBlock,
    create_memory_object_stream,
    fail_after,
    move_on_after,
    sleep,
)
from pytest import MonkeyPatch
from pytest_mock import MockerFixture, MockFixture

from apscheduler import (
    AsyncScheduler,
    CallableLookupError,
    CoalescePolicy,
    ConflictPolicy,
    DeserializationError,
    Event,
    Job,
    JobAcquired,
    JobAdded,
    JobLookupError,
    JobOutcome,
    JobReleased,
    RunState,
    Schedule,
    ScheduleAdded,
    ScheduleLookupError,
    Scheduler,
    ScheduleRemoved,
    SchedulerRole,
    SchedulerStarted,
    SchedulerStopped,
    ScheduleUpdated,
    SerializationError,
    Task,
    TaskAdded,
    TaskDefaults,
    TaskUpdated,
    current_async_scheduler,
    current_job,
    task,
)
from apscheduler._decorators import TASK_PARAMETERS_KEY, get_task_params
from apscheduler._utils import UnsetValue, qualified_name, unset
from apscheduler.abc import DataStore, Serializer
from apscheduler.datastores.base import BaseExternalDataStore
from apscheduler.datastores.memory import MemoryDataStore
from apscheduler.eventbrokers.local import LocalEventBroker
from apscheduler.executors.async_ import AsyncJobExecutor
from apscheduler.executors.subprocess import ProcessPoolJobExecutor
from apscheduler.executors.thread import ThreadPoolJobExecutor
from apscheduler.serializers.pickle import PickleSerializer
from apscheduler.triggers.combining import AndTrigger
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger

pytestmark = pytest.mark.anyio


async def dummy_async_job(delay: float = 0, fail: bool = False) -> str:
    await anyio.sleep(delay)
    if fail:
        raise RuntimeError("failing as requested")
    else:
        return "returnvalue"


def dummy_sync_job(delay: float = 0, fail: bool = False) -> str:
    time.sleep(delay)
    if fail:
        raise RuntimeError("failing as requested")
    else:
        return "returnvalue"


@task(
    job_executor="threadpool",
    max_running_jobs=3,
    misfire_grace_time=timedelta(seconds=6),
)
def decorated_job() -> None:
    pass


class DummyClass:
    def __init__(self, value: int):
        self.value = value

    @staticmethod
    async def dummy_static_method() -> str:
        return "static"

    @staticmethod
    async def dummy_async_static_method() -> str:
        return "static"

    @classmethod
    def dummy_class_method(cls) -> str:
        return "class"

    @classmethod
    async def dummy_async_class_method(cls) -> str:
        return "class"

    def dummy_instance_method(self) -> int:
        return self.value

    async def dummy_async_instance_method(self) -> int:
        return self.value

    def __call__(self) -> int:
        return self.value


class ClassMethodJob:
    @classmethod
    def run(cls, suffix: str) -> str:
        return cls.__name__ + suffix

    @classmethod
    async def run_async(cls, suffix: str) -> str:
        return cls.__name__ + suffix


class InheritedClassMethodJob(ClassMethodJob):
    pass


class DecoratedClassMethodJob:
    @classmethod
    @task(max_running_jobs=1)
    def run(cls, suffix: str, *, prefix: str, method_name: str) -> str:
        return prefix + cls.__name__ + suffix + method_name

    @classmethod
    @task(max_running_jobs=1)
    async def run_async(cls, suffix: str, *, prefix: str, method_name: str) -> str:
        return prefix + cls.__name__ + suffix + method_name


class LeftClassMethodJob(DecoratedClassMethodJob):
    pass


class RightClassMethodJob(DecoratedClassMethodJob):
    pass


class ConcurrentClassMethodJob:
    running = 0
    peak_running = 0

    @classmethod
    @task(id="shared-concurrency", max_running_jobs=1)
    async def run(cls) -> str:
        ConcurrentClassMethodJob.running += 1
        ConcurrentClassMethodJob.peak_running = max(
            ConcurrentClassMethodJob.peak_running, ConcurrentClassMethodJob.running
        )
        try:
            await sleep(0.01)
            return cls.__name__
        finally:
            ConcurrentClassMethodJob.running -= 1


class ConcurrentChildJob(ConcurrentClassMethodJob):
    pass


class TestAsyncScheduler:
    def test_repr(self) -> None:
        scheduler = AsyncScheduler(identity="my identity")
        assert repr(scheduler) == (
            "AsyncScheduler(identity='my identity', role=<SchedulerRole.both: 3>, "
            "data_store=MemoryDataStore(), event_broker=LocalEventBroker())"
        )

    async def test_use_before_initialized(self) -> None:
        scheduler = AsyncScheduler()
        with pytest.raises(
            RuntimeError, match="The scheduler has not been initialized yet"
        ):
            await scheduler.add_job(dummy_async_job)

    async def test_properties(self) -> None:
        async with AsyncScheduler() as scheduler:
            assert isinstance(scheduler.data_store, MemoryDataStore)
            assert isinstance(scheduler.event_broker, LocalEventBroker)
            assert scheduler.role is SchedulerRole.both
            assert isinstance(scheduler.identity, str)
            assert len(scheduler.job_executors) == 3
            assert isinstance(scheduler.job_executors["async"], AsyncJobExecutor)
            assert isinstance(
                scheduler.job_executors["threadpool"], ThreadPoolJobExecutor
            )
            assert isinstance(
                scheduler.job_executors["processpool"], ProcessPoolJobExecutor
            )
            assert scheduler.task_defaults.job_executor == "async"
            assert scheduler.state is RunState.stopped

    async def test_subscribe_coroutine_function(self) -> None:
        received = anyio.Event()

        async def callback(event: Event) -> None:
            received.set()

        async with AsyncScheduler() as scheduler:
            scheduler.subscribe(callback, SchedulerStarted, is_async=False)
            await scheduler.start_in_background()
            with fail_after(10):
                await received.wait()

    @pytest.mark.parametrize("as_default", [False, True])
    async def test_async_executor(self, as_default: bool) -> None:
        async with AsyncScheduler() as scheduler:
            await scheduler.start_in_background()
            if as_default:
                thread_id = await scheduler.run_job(threading.get_ident)
            else:
                thread_id = await scheduler.run_job(
                    threading.get_ident, job_executor="async"
                )

            assert thread_id == threading.get_ident()

    async def test_threadpool_executor(self) -> None:
        async with AsyncScheduler() as scheduler:
            await scheduler.start_in_background()
            thread_id = await scheduler.run_job(
                threading.get_ident, job_executor="threadpool"
            )
            assert thread_id != threading.get_ident()

    async def test_processpool_executor(self) -> None:
        async with AsyncScheduler() as scheduler:
            await scheduler.start_in_background()
            pid = await scheduler.run_job(os.getpid, job_executor="processpool")
            assert pid != os.getpid()

    async def test_configure_task(self, raw_datastore: DataStore) -> None:
        send, receive = create_memory_object_stream[Event](2)
        with send, receive:
            async with AsyncScheduler(data_store=raw_datastore) as scheduler:
                scheduler.subscribe(send.send)
                await scheduler.configure_task("mytask", func=dummy_async_job)
                await scheduler.configure_task("mytask", misfire_grace_time=2)
                tasks = await scheduler.get_tasks()
                assert len(tasks) == 1
                assert tasks[0].id == "mytask"
                assert tasks[0].func == f"{__name__}:dummy_async_job"
                assert tasks[0].misfire_grace_time == timedelta(seconds=2)

            with fail_after(3):
                event = await receive.receive()
                assert isinstance(event, TaskAdded)
                assert event.task_id == "mytask"

                event = await receive.receive()
                assert isinstance(event, TaskUpdated)
                assert event.task_id == "mytask"

    async def test_configure_task_from_task_object(self) -> None:
        # Passing a Task object must preserve its callable reference; otherwise
        # the task is stored with func=None and jobs fail to look up the
        # callable when they run (#1001).
        async with AsyncScheduler() as scheduler:
            await scheduler.configure_task(
                Task(
                    id="mytask",
                    func=f"{__name__}:dummy_async_job",
                    job_executor="async",
                )
            )
            tasks = await scheduler.get_tasks()
            assert len(tasks) == 1
            assert tasks[0].id == "mytask"
            assert tasks[0].func == f"{__name__}:dummy_async_job"
            assert tasks[0].job_executor == "async"

    async def test_configure_task_with_decorator(self) -> None:
        async with AsyncScheduler() as scheduler:
            await scheduler.configure_task("taskfunc", func=decorated_job)
            tasks = await scheduler.get_tasks()
            assert len(tasks) == 1
            assert tasks[0].max_running_jobs == 3
            assert tasks[0].misfire_grace_time == timedelta(seconds=6)
            assert tasks[0].job_executor == "threadpool"

    async def test_configure_local_task_with_decorator(self) -> None:
        @task(
            id="taskfunc",
            job_executor="threadpool",
            max_running_jobs=3,
            misfire_grace_time=timedelta(seconds=6),
            metadata={"local": 6},
        )
        def taskfunc() -> None:
            pass

        task_defaults = TaskDefaults(metadata={"global": "foo"})
        async with AsyncScheduler(task_defaults=task_defaults) as scheduler:
            await scheduler.configure_task(taskfunc, metadata={"direct": [1, 9]})
            tasks = await scheduler.get_tasks()
            assert len(tasks) == 1
            assert tasks[0].id == "taskfunc"
            assert tasks[0].max_running_jobs == 3
            assert tasks[0].misfire_grace_time == timedelta(seconds=6)
            assert tasks[0].job_executor == "threadpool"
            assert tasks[0].metadata == {"global": "foo", "local": 6, "direct": [1, 9]}

    async def test_configure_task_preserves_decorator_defaults(self) -> None:
        @task(id="taskfunc")
        def taskfunc() -> None:
            pass

        original_params = attrs.evolve(get_task_params(taskfunc))
        async with AsyncScheduler() as scheduler:
            await scheduler.configure_task(
                taskfunc,
                max_running_jobs=3,
                job_executor="threadpool",
                metadata={"first": True},
            )

        assert get_task_params(taskfunc) == original_params
        async with AsyncScheduler(
            task_defaults=TaskDefaults(max_running_jobs=4)
        ) as scheduler:
            configured_task = await scheduler.configure_task(taskfunc)
            assert configured_task.max_running_jobs == 4
            assert configured_task.job_executor == "async"
            assert configured_task.metadata == {}

    @pytest.mark.parametrize("by_id", [False, True], ids=["callable", "id"])
    @pytest.mark.parametrize(
        "decorator_settings",
        [
            {},
            {
                "job_executor": "async",
                "max_running_jobs": 7,
                "misfire_grace_time": 10,
                "metadata": {"decorated": True},
            },
        ],
        ids=["id_only", "defaults"],
    )
    @pytest.mark.parametrize(
        "changes",
        [
            {"max_running_jobs": 2},
            {"job_executor": "async"},
            {"misfire_grace_time": None},
            {"metadata": {"updated": True}},
        ],
        ids=["limit", "executor", "grace", "metadata"],
    )
    async def test_reconfigure_decorated_task_preserves_omitted_settings(
        self,
        raw_datastore: DataStore,
        by_id: bool,
        decorator_settings: dict[str, Any],
        changes: dict[str, Any],
    ) -> None:
        @task(id="shared", **decorator_settings)
        def work() -> None:
            pass

        async with AsyncScheduler(data_store=raw_datastore) as scheduler:
            configured = await scheduler.configure_task(
                work,
                job_executor="threadpool",
                max_running_jobs=3,
                misfire_grace_time=30,
                metadata={"configured": True},
            )
            expected = attrs.asdict(configured)
            expected.update(changes)
            if "metadata" in changes:
                expected["metadata"] = {**configured.metadata, **changes["metadata"]}

            updated = await scheduler.configure_task(
                "shared" if by_id else work, **changes
            )
            assert attrs.asdict(updated) == expected
            [stored] = await scheduler.get_tasks()
            assert attrs.asdict(stored) == expected

    @pytest.mark.parametrize("use_scheduling", [False, True], ids=["job", "schedule"])
    @pytest.mark.parametrize("job_executor", ["async", "threadpool"])
    async def test_generated_class_method_without_reference(
        self,
        raw_datastore: DataStore,
        serializer: Serializer,
        use_scheduling: bool,
        job_executor: str,
    ) -> None:
        @task(id="generated")
        def run(cls: type) -> str:
            return cls.__name__

        generated = type(
            "GeneratedWorker", (), {"run": classmethod(run), "__module__": "__main__"}
        )
        if isinstance(raw_datastore, BaseExternalDataStore):
            raw_datastore.serializer = serializer

        async with AsyncScheduler(data_store=raw_datastore) as scheduler:

            async def submit() -> None:
                if use_scheduling:
                    await scheduler.add_schedule(
                        generated.run,
                        DateTrigger(datetime.now(UTC)),
                        job_result_expiration_time=60,
                    )
                else:
                    await scheduler.add_job(generated.run, result_expiration_time=60)

            await scheduler.configure_task(generated.run, job_executor=job_executor)
            if not isinstance(raw_datastore, MemoryDataStore):
                with pytest.raises(SerializationError):
                    await submit()
                assert not await scheduler.get_jobs()
                assert not await scheduler.get_schedules()
                return

            await submit()
            send, receive = create_memory_object_stream[JobReleased](1)
            with send, receive, scheduler.subscribe(send.send, {JobReleased}):
                await scheduler.start_in_background()
                with fail_after(5):
                    event = await receive.receive()

            assert event.task_id == "generated"
            assert event.outcome is JobOutcome.success
            result = await scheduler.get_job_result(event.job_id)
            assert result and result.return_value == "GeneratedWorker"
            [stored] = await scheduler.get_tasks()
            assert stored.running_jobs == 0

    @pytest.mark.parametrize("use_scheduling", [False, True], ids=["job", "schedule"])
    async def test_generated_class_method_requires_explicit_task_id(
        self, use_scheduling: bool
    ) -> None:
        def run(cls: type) -> None:
            pass

        generated = type("GeneratedWorker", (), {"run": classmethod(run)})
        async with AsyncScheduler() as scheduler:
            with pytest.raises(SerializationError, match="class method"):
                if use_scheduling:
                    await scheduler.add_schedule(
                        generated.run, DateTrigger(datetime.now(UTC))
                    )
                else:
                    await scheduler.add_job(generated.run)
            assert not await scheduler.get_tasks()

    async def test_decorated_local_class_method(self) -> None:
        class LocalJob:
            @classmethod
            @task(id="local-class-method")
            async def run(cls) -> str:
                return cls.__name__

        class ChildJob(LocalJob):
            pass

        async with AsyncScheduler() as scheduler:
            await scheduler.start_in_background()
            assert await scheduler.run_job(LocalJob.run) == "LocalJob"
            assert await scheduler.run_job(ChildJob.run) == "ChildJob"

    @pytest.mark.parametrize("use_scheduling", [False, True], ids=["job", "schedule"])
    @pytest.mark.parametrize("fresh_scheduler", [False, True], ids=["same", "fresh"])
    async def test_shared_task_callable_and_id_submissions(
        self,
        raw_datastore: DataStore,
        serializer: Serializer,
        monkeypatch: MonkeyPatch,
        use_scheduling: bool,
        fresh_scheduler: bool,
    ) -> None:
        monkeypatch.setattr(
            cast(Any, DecoratedClassMethodJob.run).__func__,
            TASK_PARAMETERS_KEY,
            attrs.evolve(get_task_params(DecoratedClassMethodJob.run), id="shared"),
        )
        if isinstance(raw_datastore, BaseExternalDataStore):
            raw_datastore.serializer = serializer

        async with AsyncExitStack() as stack:
            scheduler = await stack.enter_async_context(
                AsyncScheduler(data_store=raw_datastore)
            )
            configured = await scheduler.configure_task(
                DecoratedClassMethodJob.run,
                max_running_jobs=3,
                job_executor="threadpool",
                misfire_grace_time=30,
                metadata={"configured": True},
            )
            # The first record uses the old, unmarked format. Later callable
            # submissions must not change what this record or ID submissions call.
            for target in (
                "shared",
                LeftClassMethodJob.run,
                RightClassMethodJob.run,
                "shared",
            ):
                if use_scheduling:
                    await scheduler.add_schedule(
                        target,
                        DateTrigger(datetime.now(UTC)),
                        args=("!",),
                        kwargs={"prefix": ">", "method_name": "?"},
                        job_result_expiration_time=60,
                    )
                else:
                    await scheduler.add_job(
                        target,
                        args=("!",),
                        kwargs={"prefix": ">", "method_name": "?"},
                        result_expiration_time=60,
                    )

            [stored] = await scheduler.get_tasks()
            assert attrs.asdict(stored) == attrs.asdict(configured)
            if fresh_scheduler:
                await stack.aclose()
                if isinstance(raw_datastore, BaseExternalDataStore):
                    raw_datastore.start_from_scratch = False

                scheduler = await stack.enter_async_context(
                    AsyncScheduler(data_store=raw_datastore)
                )

            send, receive = create_memory_object_stream[JobReleased](4)
            with send, receive, scheduler.subscribe(send.send, {JobReleased}):
                await scheduler.start_in_background()
                with fail_after(5):
                    events = [await receive.receive() for _ in range(4)]

            results = [await scheduler.get_job_result(e.job_id) for e in events]
            assert all(
                r is not None and r.outcome is JobOutcome.success for r in results
            )
            assert sorted(r.return_value for r in results if r) == sorted(
                ">" + name + "!?"
                for name in (
                    "DecoratedClassMethodJob",
                    "LeftClassMethodJob",
                    "RightClassMethodJob",
                    "DecoratedClassMethodJob",
                )
            )
            assert {e.task_id for e in events} == {"shared"}
            [stored] = await scheduler.get_tasks()
            assert stored.running_jobs == 0
            assert attrs.asdict(stored) == attrs.asdict(configured)

    @pytest.mark.parametrize("use_scheduling", [False, True], ids=["job", "schedule"])
    async def test_implicit_enqueue_preserves_explicit_settings(
        self, use_scheduling: bool
    ) -> None:
        async with AsyncScheduler() as scheduler:
            configured = await scheduler.configure_task(
                decorated_job,
                max_running_jobs=7,
                job_executor="async",
                misfire_grace_time=42,
                metadata={"explicit": True},
            )
            if use_scheduling:
                await scheduler.add_schedule(
                    decorated_job, DateTrigger(datetime.now(UTC))
                )
            else:
                await scheduler.add_job(decorated_job)

            [stored] = await scheduler.get_tasks()
            assert attrs.asdict(stored) == attrs.asdict(configured)
            updated = await scheduler.configure_task(configured.id, max_running_jobs=2)
            assert updated.max_running_jobs == 2
            assert updated.job_executor == "async"
            assert updated.misfire_grace_time == timedelta(seconds=42)
            assert updated.metadata == configured.metadata

    @pytest.mark.parametrize("use_scheduling", [False, True], ids=["job", "schedule"])
    async def test_class_method_explicit_configuration_lifecycle(
        self,
        raw_datastore: DataStore,
        serializer: Serializer,
        monkeypatch: MonkeyPatch,
        use_scheduling: bool,
    ) -> None:
        monkeypatch.setattr(
            cast(Any, DecoratedClassMethodJob.run).__func__,
            TASK_PARAMETERS_KEY,
            attrs.evolve(get_task_params(DecoratedClassMethodJob.run), id="shared"),
        )
        if isinstance(raw_datastore, BaseExternalDataStore):
            raw_datastore.serializer = serializer

        async with AsyncScheduler(data_store=raw_datastore) as scheduler:
            configured = await scheduler.configure_task(
                "shared",
                max_running_jobs=3,
                job_executor="threadpool",
                misfire_grace_time=30,
                metadata={"configured": True},
            )

        if isinstance(raw_datastore, BaseExternalDataStore):
            raw_datastore.start_from_scratch = False

        async with AsyncScheduler(
            data_store=raw_datastore,
            task_defaults=TaskDefaults(metadata={"different_default": True}),
        ) as scheduler:
            for target in (LeftClassMethodJob.run, "shared"):
                if use_scheduling:
                    await scheduler.add_schedule(
                        target,
                        DateTrigger(datetime.now(UTC)),
                        args=("!",),
                        kwargs={"prefix": ">", "method_name": "?"},
                        job_result_expiration_time=60,
                    )
                else:
                    await scheduler.add_job(
                        target,
                        args=("!",),
                        kwargs={"prefix": ">", "method_name": "?"},
                        result_expiration_time=60,
                    )

            [stored] = await scheduler.get_tasks()
            assert attrs.asdict(stored) == attrs.asdict(
                attrs.evolve(configured, func=f"{__name__}:LeftClassMethodJob.run")
            )
            # Explicit reconfiguration changes the default for ID submissions,
            # while the already queued callable submission retains its receiver.
            await scheduler.configure_task("shared", func=RightClassMethodJob.run)
            send, receive = create_memory_object_stream[JobReleased](2)
            with send, receive, scheduler.subscribe(send.send, {JobReleased}):
                await scheduler.start_in_background()
                with fail_after(5):
                    events = [await receive.receive(), await receive.receive()]

            assert all(e.outcome is JobOutcome.success for e in events)
            results = [await scheduler.get_job_result(e.job_id) for e in events]
            assert {r.return_value for r in results if r} == {
                ">LeftClassMethodJob!?",
                ">RightClassMethodJob!?",
            }

    @pytest.mark.parametrize("use_scheduling", [False, True], ids=["job", "schedule"])
    @pytest.mark.parametrize("job_executor", ["async", "threadpool", "processpool"])
    async def test_class_methods_sqlite_new_process(
        self,
        tmp_path: Path,
        serializer: Serializer,
        monkeypatch: MonkeyPatch,
        use_scheduling: bool,
        job_executor: str,
    ) -> None:
        from apscheduler._marshalling import callable_to_ref
        from apscheduler.datastores.sqlalchemy import SQLAlchemyDataStore

        monkeypatch.setattr(
            cast(Any, DecoratedClassMethodJob.run).__func__,
            TASK_PARAMETERS_KEY,
            attrs.evolve(get_task_params(DecoratedClassMethodJob.run), id="shared"),
        )
        url = f"sqlite+aiosqlite:///{tmp_path}/restart.db"
        async with AsyncScheduler(
            data_store=SQLAlchemyDataStore(url, serializer=serializer)
        ) as scheduler:
            await scheduler.configure_task(
                "shared", func=DecoratedClassMethodJob.run, job_executor=job_executor
            )
            for target in ("shared", LeftClassMethodJob.run, RightClassMethodJob.run):
                if use_scheduling:
                    await scheduler.add_schedule(
                        target,
                        DateTrigger(datetime.now(UTC)),
                        args=("!",),
                        kwargs={"prefix": ">", "method_name": "?"},
                        job_result_expiration_time=60,
                    )
                else:
                    await scheduler.add_job(
                        target,
                        args=("!",),
                        kwargs={"prefix": ">", "method_name": "?"},
                        result_expiration_time=60,
                    )

        # A new interpreter, store, engine and callable cache read both unmarked
        # and marked persisted records. No producer Python objects are reused.
        worker = """
import sys, json, anyio
sys.path.insert(0, sys.argv[3])
from apscheduler import AsyncScheduler, JobReleased, JobOutcome
from apscheduler._marshalling import callable_from_ref
from apscheduler.datastores.sqlalchemy import SQLAlchemyDataStore

async def main():
    store = SQLAlchemyDataStore(sys.argv[1], serializer=callable_from_ref(sys.argv[2])())
    async with AsyncScheduler(data_store=store) as scheduler:
        send, receive = anyio.create_memory_object_stream[JobReleased](3)
        with send, receive, scheduler.subscribe(send.send, {JobReleased}):
            await scheduler.start_in_background()
            with anyio.fail_after(15):
                events = [await receive.receive() for _ in range(3)]
        assert all(e.outcome is JobOutcome.success for e in events)
        assert {e.task_id for e in events} == {"shared"}
        results = [await scheduler.get_job_result(e.job_id) for e in events]
        print(json.dumps(sorted(r.return_value for r in results)))
        [task] = await scheduler.get_tasks()
        assert task.running_jobs == 0

anyio.run(main)
"""
        with fail_after(30):
            completed = await anyio.run_process(
                [
                    sys.executable,
                    "-c",
                    worker,
                    url,
                    callable_to_ref(type(serializer)),
                    str(Path(__file__).parent),
                ],
                check=False,
            )
        assert completed.returncode == 0, completed.stderr.decode()
        assert json.loads(completed.stdout) == [
            ">DecoratedClassMethodJob!?",
            ">LeftClassMethodJob!?",
            ">RightClassMethodJob!?",
        ]

    @pytest.mark.parametrize("use_scheduling", [False, True], ids=["job", "schedule"])
    @pytest.mark.parametrize(
        "bad_ref",
        [
            "invalid-reference",
            "missing_invocation_module:run",
            f"{__name__}:missing_method",
            f"{__name__}:__name__",
        ],
    )
    @pytest.mark.parametrize("invalid_default", [False, True])
    async def test_invocation_lookup_failure_releases_job(
        self,
        raw_datastore: DataStore,
        serializer: Serializer,
        use_scheduling: bool,
        bad_ref: str,
        invalid_default: bool,
    ) -> None:
        if isinstance(raw_datastore, BaseExternalDataStore):
            raw_datastore.serializer = serializer

        async with AsyncScheduler(
            data_store=raw_datastore, max_concurrent_jobs=1
        ) as scheduler:
            await scheduler.configure_task("shared", func=ClassMethodJob.run)
            expected_ref = bad_ref
            if invalid_default:
                # An unmarked record uses the normal task resolver and fails;
                # a marked record must still run even if Task.func is invalid.
                bad_task_ref = (
                    bad_ref if ":" in bad_ref else f"{__name__}:missing_method"
                )
                expected_ref = bad_task_ref
                stored = await scheduler.data_store.get_task("shared")
                await scheduler.data_store.add_task(
                    attrs.evolve(stored, func=bad_task_ref)
                )
                scheduler._task_callables.clear()

            invocation_func = None if invalid_default else bad_ref
            invocation_args: Any = ("!",)
            if use_scheduling:
                trigger = DateTrigger(datetime.now(UTC))
                next_fire_time = trigger.next()
                assert next_fire_time is not None
                await scheduler.data_store.add_schedule(
                    Schedule(
                        id="bad",
                        task_id="shared",
                        trigger=trigger,
                        next_fire_time=next_fire_time,
                        job_executor="async",
                        args=invocation_args,
                        func=invocation_func,
                        job_result_expiration_time=60,
                    ),
                    ConflictPolicy.exception,
                )
            else:
                await scheduler.data_store.add_job(
                    Job(
                        task_id="shared",
                        executor="async",
                        args=invocation_args,
                        func=invocation_func,
                        result_expiration_time=60,
                    )
                )

            send, receive = create_memory_object_stream[JobReleased](2)
            with send, receive, scheduler.subscribe(send.send, {JobReleased}):
                await scheduler.start_in_background()
                with fail_after(5):
                    failed = await receive.receive()
                    assert failed.outcome is JobOutcome.error
                    # No acquired-job slot or shared-task concurrency slot may leak.
                    if invalid_default:
                        good = Job(
                            task_id="shared",
                            executor="async",
                            args=invocation_args,
                            func=f"{__name__}:ClassMethodJob.run",
                            result_expiration_time=60,
                        )
                        await scheduler.data_store.add_job(good)
                        good_id = good.id
                    else:
                        good_id = await scheduler.add_job(
                            "shared", args=("!",), result_expiration_time=60
                        )

                    completed = await receive.receive()

            assert completed.job_id == good_id
            assert completed.outcome is JobOutcome.success
            result = await scheduler.get_job_result(good_id, wait=False)
            assert result and result.return_value == "ClassMethodJob!"
            # Check lookup diagnostics without coupling worker recovery to
            # deserialization of the stored exception object.
            assert failed.task_id == "shared"
            assert failed.started_at is not None
            assert failed.exception_type in {
                qualified_name(exception_type)
                for exception_type in (
                    ValueError,
                    LookupError,
                    DeserializationError,
                    CallableLookupError,
                )
            }
            assert failed.exception_message is not None
            assert expected_ref in failed.exception_message
            [stored] = await scheduler.get_tasks()
            assert stored.running_jobs == 0
            assert scheduler.state is RunState.started

    @pytest.mark.parametrize("use_scheduling", [False, True], ids=["job", "schedule"])
    async def test_class_method_shared_concurrency(
        self,
        raw_datastore: DataStore,
        serializer: Serializer,
        use_scheduling: bool,
        monkeypatch: MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(ConcurrentClassMethodJob, "running", 0)
        monkeypatch.setattr(ConcurrentClassMethodJob, "peak_running", 0)
        if isinstance(raw_datastore, BaseExternalDataStore):
            raw_datastore.serializer = serializer

        async with AsyncScheduler(data_store=raw_datastore) as scheduler:
            for target in (ConcurrentClassMethodJob.run, ConcurrentChildJob.run):
                if use_scheduling:
                    await scheduler.add_schedule(
                        target,
                        DateTrigger(datetime.now(UTC)),
                        job_result_expiration_time=60,
                    )
                else:
                    await scheduler.add_job(target, result_expiration_time=60)

            send, receive = create_memory_object_stream[JobReleased](2)
            with send, receive, scheduler.subscribe(send.send, {JobReleased}):
                await scheduler.start_in_background()
                with fail_after(5):
                    events = [await receive.receive(), await receive.receive()]

            assert {e.task_id for e in events} == {"shared-concurrency"}
            assert all(e.outcome is JobOutcome.success for e in events)
            assert ConcurrentClassMethodJob.peak_running == 1
            [stored] = await scheduler.get_tasks()
            assert stored.max_running_jobs == 1
            assert stored.running_jobs == 0

    @pytest.mark.parametrize("use_scheduling", [False, True], ids=["job", "schedule"])
    async def test_base_era_class_argument_is_not_reinterpreted(
        self, raw_datastore: DataStore, use_scheduling: bool
    ) -> None:
        if isinstance(raw_datastore, BaseExternalDataStore):
            raw_datastore.serializer = PickleSerializer()

        async with AsyncScheduler(data_store=raw_datastore) as scheduler:
            await scheduler.configure_task("legacy", func=ClassMethodJob.run)
            invocation_args: Any = (ClassMethodJob, "!")
            # This is the ambiguous pre-PR format. Its explicit first argument
            # must not be silently removed, even though this particular call fails.
            if use_scheduling:
                trigger = DateTrigger(datetime.now(UTC))
                next_fire_time = trigger.next()
                assert next_fire_time is not None
                await scheduler.data_store.add_schedule(
                    Schedule(
                        id="legacy",
                        task_id="legacy",
                        trigger=trigger,
                        next_fire_time=next_fire_time,
                        job_executor="async",
                        args=invocation_args,
                        job_result_expiration_time=60,
                    ),
                    ConflictPolicy.exception,
                )
            else:
                await scheduler.data_store.add_job(
                    Job(
                        task_id="legacy",
                        executor="async",
                        args=invocation_args,
                        result_expiration_time=60,
                    )
                )

            send, receive = create_memory_object_stream[JobReleased](2)
            with send, receive, scheduler.subscribe(send.send, {JobReleased}):
                await scheduler.start_in_background()
                with fail_after(5):
                    failed = await receive.receive()
                    assert failed.outcome is JobOutcome.error
                    result = await scheduler.get_job_result(failed.job_id)
                    assert result and isinstance(result.exception, TypeError)
                    good_id = await scheduler.add_job(
                        "legacy", args=("!",), result_expiration_time=60
                    )
                    completed = await receive.receive()

            assert completed.job_id == good_id
            assert completed.outcome is JobOutcome.success
            result = await scheduler.get_job_result(good_id)
            assert result and result.return_value == "ClassMethodJob!"
            [stored] = await scheduler.get_tasks()
            assert stored.running_jobs == 0

    async def test_add_pause_unpause_remove_schedule(
        self, raw_datastore: DataStore, timezone: ZoneInfo
    ) -> None:
        send, receive = create_memory_object_stream[Event](5)
        with send, receive:
            async with AsyncScheduler(data_store=raw_datastore) as scheduler:
                scheduler.subscribe(send.send)
                now = datetime.now(timezone)
                trigger = DateTrigger(now)
                schedule_id = await scheduler.add_schedule(
                    dummy_async_job, trigger, id="foo"
                )
                assert schedule_id == "foo"

                schedules = await scheduler.get_schedules()
                assert len(schedules) == 1
                assert schedules[0].id == "foo"
                assert schedules[0].task_id == f"{__name__}:dummy_async_job"

                await scheduler.pause_schedule("foo")
                schedule = await scheduler.get_schedule("foo")
                assert schedule.paused
                assert schedule.next_fire_time == now

                await scheduler.unpause_schedule("foo")
                schedule = await scheduler.get_schedule("foo")
                assert not schedule.paused
                assert schedule.next_fire_time == now

                await scheduler.remove_schedule(schedule_id)
                assert not await scheduler.get_schedules()

            with fail_after(3):
                event = await receive.receive()
                assert isinstance(event, TaskAdded)
                assert event.task_id == f"{__name__}:dummy_async_job"

                event = await receive.receive()
                assert isinstance(event, ScheduleAdded)
                assert event.schedule_id == "foo"
                assert event.task_id == f"{__name__}:dummy_async_job"
                assert event.next_fire_time == now

                event = await receive.receive()
                assert isinstance(event, ScheduleUpdated)
                assert event.schedule_id == "foo"
                assert event.task_id == f"{__name__}:dummy_async_job"
                assert event.next_fire_time == now

                event = await receive.receive()
                assert isinstance(event, ScheduleUpdated)
                assert event.schedule_id == "foo"
                assert event.task_id == f"{__name__}:dummy_async_job"
                assert event.next_fire_time == now

                event = await receive.receive()
                assert isinstance(event, ScheduleRemoved)
                assert event.schedule_id == "foo"
                assert event.task_id == f"{__name__}:dummy_async_job"
                assert not event.finished

    async def test_add_job_wait_result(self, raw_datastore: DataStore) -> None:
        send, receive = create_memory_object_stream[Event](2)
        async with AsyncExitStack() as exit_stack:
            exit_stack.enter_context(send)
            exit_stack.enter_context(receive)
            scheduler = await exit_stack.enter_async_context(
                AsyncScheduler(data_store=raw_datastore)
            )
            assert await scheduler.get_jobs() == []

            scheduler.subscribe(send.send)
            job_id = await scheduler.add_job(dummy_async_job, result_expiration_time=10)

            with fail_after(3):
                event = await receive.receive()
                assert isinstance(event, TaskAdded)
                assert event.task_id == f"{__name__}:dummy_async_job"

                event = await receive.receive()
                assert isinstance(event, JobAdded)
                assert event.job_id == job_id

            jobs = await scheduler.get_jobs()
            assert len(jobs) == 1
            assert jobs[0].id == job_id
            assert jobs[0].task_id == f"{__name__}:dummy_async_job"

            with pytest.raises(JobLookupError):
                await scheduler.get_job_result(job_id, wait=False)

            await scheduler.start_in_background()

            with fail_after(3):
                event = await receive.receive()
                assert isinstance(event, SchedulerStarted)

                event = await receive.receive()
                assert isinstance(event, JobAcquired)
                assert event.job_id == job_id
                assert event.task_id == f"{__name__}:dummy_async_job"
                assert event.schedule_id is None
                assert event.scheduled_start is None
                acquired_at = event.timestamp

                event = await receive.receive()
                assert isinstance(event, JobReleased)
                assert event.job_id == job_id
                assert event.task_id == f"{__name__}:dummy_async_job"
                assert event.schedule_id is None
                assert event.scheduled_start is None
                assert event.started_at is not None
                assert event.started_at >= acquired_at
                assert event.outcome is JobOutcome.success

                result = await scheduler.get_job_result(job_id)
                assert result
                assert result.outcome is JobOutcome.success
                assert result.return_value == "returnvalue"

    @pytest.mark.parametrize("success", [True, False])
    async def test_run_job(self, raw_datastore: DataStore, success: bool) -> None:
        send, receive = create_memory_object_stream[Event](4)
        with send, receive:
            async with AsyncScheduler(data_store=raw_datastore) as scheduler:
                await scheduler.start_in_background()
                scheduler.subscribe(send.send)
                try:
                    result = await scheduler.run_job(
                        dummy_async_job, kwargs={"fail": not success}
                    )
                except RuntimeError as exc:
                    assert str(exc) == "failing as requested"
                else:
                    assert result == "returnvalue"

                assert not await scheduler.get_jobs()

                with fail_after(3):
                    # The task was added
                    event = await receive.receive()
                    assert isinstance(event, TaskAdded)
                    assert event.task_id == f"{__name__}:dummy_async_job"

                    # The job was added
                    event = await receive.receive()
                    assert isinstance(event, JobAdded)
                    job_id = event.job_id
                    assert event.task_id == f"{__name__}:dummy_async_job"

                    # The scheduler acquired the job
                    event = await receive.receive()
                    assert isinstance(event, JobAcquired)
                    assert event.job_id == job_id
                    assert event.task_id == f"{__name__}:dummy_async_job"
                    assert event.schedule_id is None
                    assert event.scheduled_start is None
                    assert event.scheduler_id == scheduler.identity
                    acquired_at = event.timestamp

                    # The scheduler released the job
                    event = await receive.receive()
                    assert isinstance(event, JobReleased)
                    assert event.job_id == job_id
                    assert event.task_id == f"{__name__}:dummy_async_job"
                    assert event.schedule_id is None
                    assert event.scheduled_start is None
                    assert event.started_at is not None
                    assert event.started_at >= acquired_at
                    assert event.scheduler_id == scheduler.identity

            # The scheduler was stopped
            event = await receive.receive()
            assert isinstance(event, SchedulerStopped)

            # There should be no more events on the list
            with pytest.raises(WouldBlock):
                receive.receive_nowait()

    @pytest.mark.parametrize(
        "target, expected_result",
        [
            pytest.param(dummy_async_job, "returnvalue", id="async_func"),
            pytest.param(dummy_sync_job, "returnvalue", id="sync_func"),
            pytest.param(DummyClass.dummy_static_method, "static", id="staticmethod"),
            pytest.param(
                DummyClass.dummy_async_static_method, "static", id="async_staticmethod"
            ),
            pytest.param(DummyClass.dummy_class_method, "class", id="classmethod"),
            pytest.param(
                DummyClass.dummy_async_class_method, "class", id="async_classmethod"
            ),
            pytest.param(DummyClass(5).dummy_instance_method, 5, id="instancemethod"),
            pytest.param(
                DummyClass(6).dummy_async_instance_method, 6, id="async_instancemethod"
            ),
            pytest.param(bytes, b"", id="builtin_function"),
            pytest.param(
                datetime(2023, 10, 19, tzinfo=UTC).timestamp,
                1697673600.0,
                id="builtin_method",
            ),
            pytest.param(partial(bytes, "foo", "ascii"), b"foo", id="partial"),
        ],
    )
    @pytest.mark.parametrize(
        "use_scheduling",
        [
            pytest.param(False, id="job"),
            pytest.param(True, id="schedule"),
        ],
    )
    async def test_callable_types(
        self,
        target: Callable[..., Any],
        expected_result: object,
        use_scheduling: bool,
        raw_datastore: DataStore,
        timezone: ZoneInfo,
    ) -> None:
        now = datetime.now(timezone)
        send, receive = create_memory_object_stream[Event](4)
        async with AsyncExitStack() as exit_stack:
            exit_stack.enter_context(send)
            exit_stack.enter_context(receive)
            scheduler = await exit_stack.enter_async_context(
                AsyncScheduler(data_store=raw_datastore)
            )
            scheduler.subscribe(send.send, {JobReleased})
            await scheduler.start_in_background()
            if use_scheduling:
                trigger = DateTrigger(now)
                await scheduler.add_schedule(target, trigger, id="foo")
            else:
                await scheduler.add_job(target, result_expiration_time=10)

            with fail_after(3):
                event = await receive.receive()
                assert isinstance(event, JobReleased)

            if not use_scheduling:
                result = await scheduler.get_job_result(event.job_id)
                assert result
                assert result.outcome is JobOutcome.success
                assert result.return_value == expected_result

    @pytest.mark.parametrize("job_class", [ClassMethodJob, InheritedClassMethodJob])
    @pytest.mark.parametrize("method_name", ["run", "run_async"])
    @pytest.mark.parametrize("use_scheduling", [False, True], ids=["job", "schedule"])
    async def test_class_method_new_scheduler(
        self,
        job_class: type[ClassMethodJob],
        method_name: str,
        use_scheduling: bool,
        raw_datastore: DataStore,
    ) -> None:
        async with AsyncScheduler(data_store=raw_datastore) as scheduler:
            target = getattr(job_class, method_name)
            if use_scheduling:
                await scheduler.add_schedule(
                    target,
                    DateTrigger(datetime.now(UTC)),
                    args=("!",),
                    job_result_expiration_time=10,
                )
            else:
                await scheduler.add_job(target, args=("!",), result_expiration_time=10)

        if isinstance(raw_datastore, BaseExternalDataStore):
            raw_datastore.start_from_scratch = False

        async with AsyncScheduler(data_store=raw_datastore) as scheduler:
            send, receive = create_memory_object_stream[JobReleased](1)
            with send, receive, scheduler.subscribe(send.send, {JobReleased}):
                await scheduler.start_in_background()
                with fail_after(3):
                    event = await receive.receive()

            result = await scheduler.get_job_result(event.job_id)
            assert result is not None
            assert result.outcome is JobOutcome.success
            assert result.return_value == job_class.__name__ + "!"

    @pytest.mark.parametrize("method_name", ["run", "run_async"])
    @pytest.mark.parametrize("use_scheduling", [False, True], ids=["job", "schedule"])
    @pytest.mark.parametrize("fresh_scheduler", [False, True], ids=["same", "fresh"])
    async def test_cross_module_inherited_class_methods(
        self,
        cross_module_job_classes: tuple[type, type],
        method_name: str,
        use_scheduling: bool,
        fresh_scheduler: bool,
        raw_datastore: DataStore,
    ) -> None:
        async with AsyncExitStack() as exit_stack:
            scheduler = await exit_stack.enter_async_context(
                AsyncScheduler(data_store=raw_datastore)
            )
            for job_class in cross_module_job_classes:
                target = getattr(job_class, method_name)
                if use_scheduling:
                    await scheduler.add_schedule(
                        target,
                        DateTrigger(datetime.now(UTC)),
                        job_result_expiration_time=10,
                    )
                else:
                    await scheduler.add_job(target, result_expiration_time=10)

            if fresh_scheduler:
                await exit_stack.aclose()
                if isinstance(raw_datastore, BaseExternalDataStore):
                    raw_datastore.start_from_scratch = False

                scheduler = await exit_stack.enter_async_context(
                    AsyncScheduler(data_store=raw_datastore)
                )

            send, receive = create_memory_object_stream[JobReleased](2)
            with send, receive, scheduler.subscribe(send.send, {JobReleased}):
                await scheduler.start_in_background()
                with fail_after(3):
                    events = [await receive.receive(), await receive.receive()]

            results = []
            for event in events:
                result = await scheduler.get_job_result(event.job_id)
                assert result is not None
                results.append(result)

            assert all(result.outcome is JobOutcome.success for result in results)
            identities = {
                f"{job_class.__module__}:{job_class.__qualname__}"
                for job_class in cross_module_job_classes
            }
            assert {result.return_value for result in results} == identities
            expected_refs = {f"{identity}.{method_name}" for identity in identities}
            assert {event.task_id for event in events} == expected_refs
            tasks = await scheduler.get_tasks()
            assert {task.id for task in tasks} == expected_refs
            assert {task.func for task in tasks} == expected_refs

    @pytest.mark.parametrize("task_id", [unset, "shared"], ids=["implicit", "explicit"])
    @pytest.mark.parametrize(
        "method_name, job_executor",
        [
            pytest.param("run", "async", id="sync"),
            pytest.param("run", "threadpool", id="threadpool"),
            pytest.param("run", "processpool", id="processpool"),
            pytest.param("run_async", "async", id="async"),
        ],
    )
    @pytest.mark.parametrize("use_scheduling", [False, True], ids=["job", "schedule"])
    @pytest.mark.parametrize("fresh_scheduler", [False, True], ids=["same", "fresh"])
    @pytest.mark.parametrize("use_partial", [False, True], ids=["direct", "partial"])
    async def test_decorated_inherited_class_methods(
        self,
        task_id: str | UnsetValue,
        method_name: str,
        job_executor: str,
        use_scheduling: bool,
        fresh_scheduler: bool,
        use_partial: bool,
        raw_datastore: DataStore,
        serializer: Serializer,
        monkeypatch: MonkeyPatch,
    ) -> None:
        target = getattr(DecoratedClassMethodJob, method_name)
        params = attrs.evolve(get_task_params(target), id=task_id)
        monkeypatch.setattr(target.__func__, TASK_PARAMETERS_KEY, params)
        job_classes = (LeftClassMethodJob, RightClassMethodJob)
        if isinstance(raw_datastore, BaseExternalDataStore):
            raw_datastore.serializer = serializer

        task_defaults = TaskDefaults(job_executor=job_executor)
        async with AsyncExitStack() as exit_stack:
            scheduler = await exit_stack.enter_async_context(
                AsyncScheduler(data_store=raw_datastore, task_defaults=task_defaults)
            )
            for job_class in job_classes:
                target = getattr(job_class, method_name)
                if use_partial:
                    target = partial(target, prefix=">")

                if use_scheduling:
                    await scheduler.add_schedule(
                        target,
                        DateTrigger(datetime.now(UTC)),
                        args=("!",),
                        kwargs={"prefix": ">", "method_name": "?"},
                        job_result_expiration_time=10,
                    )
                else:
                    await scheduler.add_job(
                        target,
                        args=("!",),
                        kwargs={"prefix": ">", "method_name": "?"},
                        result_expiration_time=10,
                    )

            if fresh_scheduler:
                await exit_stack.aclose()
                if isinstance(raw_datastore, BaseExternalDataStore):
                    raw_datastore.start_from_scratch = False

                scheduler = await exit_stack.enter_async_context(
                    AsyncScheduler(
                        data_store=raw_datastore, task_defaults=task_defaults
                    )
                )

            events: list[JobReleased] = []

            async def job_released(event: JobReleased) -> None:
                events.append(event)
                if len(events) == len(job_classes):
                    await scheduler.stop()

            with scheduler.subscribe(job_released, {JobReleased}):
                # Completion drives shutdown. Bound the whole run (including
                # SQLite I/O and process startup), rather than timing event delivery.
                with fail_after(30):
                    await scheduler.run_until_stopped()

            assert len(events) == len(job_classes)
            assert all(event.outcome is JobOutcome.success for event in events)
            results = []
            for event in events:
                result = await scheduler.get_job_result(event.job_id, wait=False)
                assert result is not None
                results.append(result)
            assert all(result.outcome is JobOutcome.success for result in results)
            assert {result.return_value for result in results} == {
                ">" + job_class.__name__ + "!?" for job_class in job_classes
            }
            expected_ids = (
                {task_id}
                if isinstance(task_id, str)
                else {
                    f"{__name__}:{job_class.__qualname__}.{method_name}"
                    for job_class in job_classes
                }
            )
            tasks = await scheduler.get_tasks()
            assert {task.id for task in tasks} == expected_ids
            assert {event.task_id for event in events} == expected_ids
            assert all(task.max_running_jobs == 1 for task in tasks)
            assert all(task.job_executor == job_executor for task in tasks)
            assert all(task.running_jobs == 0 for task in tasks)
            assert not scheduler._running_jobs
            assert scheduler.state is RunState.stopped
            assert params.id is task_id

    async def test_scheduled_job_missed_deadline(
        self, raw_datastore: DataStore, timezone: ZoneInfo
    ) -> None:
        one_second_in_past = datetime.now(timezone) - timedelta(seconds=1)
        trigger = DateTrigger(one_second_in_past)
        scheduler_send, scheduler_receive = create_memory_object_stream[Event](4)
        worker_send, worker_receive = create_memory_object_stream[Event](2)
        with scheduler_send, scheduler_receive, worker_send, worker_receive:
            async with AsyncExitStack() as exit_stack:
                scheduler = await exit_stack.enter_async_context(
                    AsyncScheduler(data_store=raw_datastore)
                )
                await scheduler.add_schedule(
                    dummy_async_job, trigger, misfire_grace_time=0, id="foo"
                )
                await scheduler.start_in_background()
                exit_stack.enter_context(
                    scheduler.subscribe(
                        scheduler_send.send, {JobAdded, ScheduleUpdated}
                    )
                )
                exit_stack.enter_context(
                    scheduler.subscribe(worker_send.send, {JobAcquired, JobReleased})
                )
                with fail_after(3):
                    # The schedule was processed and a job was added for it
                    event = await scheduler_receive.receive()
                    assert isinstance(event, JobAdded)
                    assert event.schedule_id == "foo"
                    assert event.task_id == "test_schedulers:dummy_async_job"
                    job_id = event.job_id

                    # The schedule was updated with a null next fire time
                    event = await scheduler_receive.receive()
                    assert isinstance(event, ScheduleUpdated)
                    assert event.schedule_id == "foo"
                    assert event.next_fire_time is None

                    # The new job was acquired
                    event = await worker_receive.receive()
                    assert isinstance(event, JobReleased)
                    assert event.job_id == job_id
                    assert event.task_id == "test_schedulers:dummy_async_job"
                    assert event.schedule_id == "foo"
                    assert event.scheduled_start == one_second_in_past
                    assert event.started_at is None
                    assert event.outcome is JobOutcome.missed_start_deadline

            # There should be no more events on the list
            with pytest.raises(WouldBlock):
                scheduler_receive.receive_nowait()

    @pytest.mark.parametrize(
        "coalesce, expected_jobs, first_fire_time_delta",
        [
            pytest.param(
                CoalescePolicy.all, 4, timedelta(minutes=3, seconds=5), id="all"
            ),
            pytest.param(
                CoalescePolicy.earliest,
                1,
                timedelta(minutes=3, seconds=5),
                id="earliest",
            ),
            pytest.param(CoalescePolicy.latest, 1, timedelta(seconds=5), id="latest"),
        ],
    )
    async def test_coalesce_policy(
        self,
        coalesce: CoalescePolicy,
        expected_jobs: int,
        first_fire_time_delta: timedelta,
        raw_datastore: DataStore,
        timezone: ZoneInfo,
    ) -> None:
        now = datetime.now(timezone)
        first_start_time = now - timedelta(minutes=3, seconds=5)
        trigger = IntervalTrigger(minutes=1, start_time=first_start_time)
        send, receive = create_memory_object_stream[Event](4)
        with send, receive:
            async with AsyncScheduler(
                data_store=raw_datastore,
                role=SchedulerRole.scheduler,
                cleanup_interval=None,
            ) as scheduler:
                await scheduler.add_schedule(
                    dummy_async_job, trigger, id="foo", coalesce=coalesce
                )
                scheduler.subscribe(send.send)
                await scheduler.start_in_background()

                with fail_after(3):
                    # The scheduler was started
                    event = await receive.receive()
                    assert isinstance(event, SchedulerStarted)

                    # The schedule was processed and one or more jobs weres added
                    for _ in range(expected_jobs):
                        event = await receive.receive()
                        assert isinstance(event, JobAdded)
                        assert event.schedule_id == "foo"
                        assert event.task_id == "test_schedulers:dummy_async_job"

                    event = await receive.receive()
                    assert isinstance(event, ScheduleUpdated)
                    assert event.next_fire_time == now + timedelta(seconds=55)

                expected_scheduled_fire_time = now - first_fire_time_delta
                jobs = await scheduler.get_jobs()
                for job in sorted(jobs, key=lambda job: job.scheduled_fire_time):
                    assert job.scheduled_fire_time
                    assert job.scheduled_fire_time < now
                    assert job.scheduled_fire_time == expected_scheduled_fire_time
                    expected_scheduled_fire_time += timedelta(minutes=1)

            # The scheduler was stopped
            event = await receive.receive()
            assert isinstance(event, SchedulerStopped)

            # There should be no more events on the list
            with pytest.raises(WouldBlock):
                receive.receive_nowait()

    async def test_trigger_error(self) -> None:
        start_time = datetime.now(UTC) - timedelta(seconds=5)
        # Yields one fire time, then raises MaxIterationsReached on the next call
        failing_trigger = AndTrigger(
            [
                IntervalTrigger(minutes=2, start_time=start_time),
                IntervalTrigger(minutes=3, start_time=start_time),
            ],
            max_iterations=1,
        )
        send, receive = create_memory_object_stream[ScheduleUpdated](2)
        with send, receive:
            async with AsyncScheduler(role=SchedulerRole.scheduler) as scheduler:
                await scheduler.add_schedule(
                    dummy_async_job,
                    IntervalTrigger(minutes=1, start_time=start_time),
                    id="ok",
                )
                await scheduler.add_schedule(
                    dummy_async_job, failing_trigger, id="failing"
                )
                scheduler.subscribe(send.send, ScheduleUpdated)
                await scheduler.start_in_background()
                with fail_after(3):
                    events = {}
                    for _ in range(2):
                        event = await receive.receive()
                        events[event.schedule_id] = event

                assert events["ok"].next_fire_time is not None
                assert events["failing"].next_fire_time is None
                # The already-due run of the failing schedule must still be queued
                jobs = await scheduler.get_jobs()
                assert {job.schedule_id for job in jobs} == {"ok", "failing"}

    @pytest.mark.parametrize(
        "max_jitter, expected_upper_bound",
        [pytest.param(2, 2, id="within"), pytest.param(4, 2.999999, id="exceed")],
    )
    async def test_jitter(
        self,
        mocker: MockerFixture,
        timezone: ZoneInfo,
        max_jitter: float,
        expected_upper_bound: float,
        raw_datastore: DataStore,
    ) -> None:
        jitter = 1.569374
        now = datetime.now(timezone)
        fake_uniform = mocker.patch("random.uniform")
        fake_uniform.configure_mock(side_effect=lambda a, b: jitter)
        send, receive = create_memory_object_stream[Event](4)
        async with AsyncExitStack() as exit_stack:
            exit_stack.enter_context(send)
            exit_stack.enter_context(receive)
            scheduler = await exit_stack.enter_async_context(
                AsyncScheduler(data_store=raw_datastore, role=SchedulerRole.scheduler)
            )
            scheduler.subscribe(send.send)
            trigger = DateTrigger(now)
            schedule_id = await scheduler.add_schedule(
                dummy_async_job, trigger, id="foo", max_jitter=max_jitter
            )
            schedule = await scheduler.get_schedule(schedule_id)
            assert schedule.max_jitter == timedelta(seconds=max_jitter)

            await scheduler.start_in_background()

            with fail_after(3):
                # The task was added
                event = await receive.receive()
                assert isinstance(event, TaskAdded)
                assert event.task_id == "test_schedulers:dummy_async_job"

                # The schedule was added
                event = await receive.receive()
                assert isinstance(event, ScheduleAdded)
                assert event.schedule_id == "foo"
                assert event.next_fire_time == now

                # The scheduler was started
                event = await receive.receive()
                assert isinstance(event, SchedulerStarted)

                # The schedule was processed and a job was added for it
                event = await receive.receive()
                assert isinstance(event, JobAdded)
                assert event.schedule_id == "foo"
                assert event.task_id == "test_schedulers:dummy_async_job"

                # Check that the job was created with the proper amount of jitter in its
                # scheduled time
                jobs = await scheduler.get_jobs()
                assert len(jobs) == 1
                assert jobs[0].jitter == timedelta(seconds=jitter)
                assert jobs[0].scheduled_fire_time == now + timedelta(seconds=jitter)
                assert jobs[0].original_scheduled_time == now

    async def test_add_job_get_result_success(self, raw_datastore: DataStore) -> None:
        async with AsyncScheduler(data_store=raw_datastore) as scheduler:
            job_id = await scheduler.add_job(
                dummy_async_job, kwargs={"delay": 0.2}, result_expiration_time=5
            )
            await scheduler.start_in_background()
            with fail_after(3):
                result = await scheduler.get_job_result(job_id)

            assert result
            assert result.job_id == job_id
            assert result.outcome is JobOutcome.success
            assert result.return_value == "returnvalue"

    async def test_add_job_get_result_empty(self, raw_datastore: DataStore) -> None:
        send, receive = create_memory_object_stream[Event](4)
        async with AsyncExitStack() as exit_stack:
            exit_stack.enter_context(send)
            exit_stack.enter_context(receive)
            scheduler = await exit_stack.enter_async_context(
                AsyncScheduler(data_store=raw_datastore)
            )
            await scheduler.start_in_background()

            scheduler.subscribe(send.send)
            job_id = await scheduler.add_job(dummy_async_job)

            with fail_after(3):
                event = await receive.receive()
                assert isinstance(event, TaskAdded)

                event = await receive.receive()
                assert isinstance(event, JobAdded)
                assert event.job_id == job_id

                event = await receive.receive()
                assert isinstance(event, JobAcquired)
                assert event.job_id == job_id
                assert event.task_id == "test_schedulers:dummy_async_job"
                assert event.schedule_id is None

                event = await receive.receive()
                assert isinstance(event, JobReleased)
                assert event.job_id == job_id
                assert event.task_id == "test_schedulers:dummy_async_job"
                assert event.schedule_id is None

            with pytest.raises(JobLookupError):
                await scheduler.get_job_result(job_id, wait=False)

    async def test_add_job_get_result_error(self) -> None:
        async with AsyncScheduler() as scheduler:
            job_id = await scheduler.add_job(
                dummy_async_job,
                kwargs={"delay": 0.2, "fail": True},
                result_expiration_time=5,
            )
            await scheduler.start_in_background()
            with fail_after(3):
                result = await scheduler.get_job_result(job_id)

            assert result
            assert result.job_id == job_id
            assert result.outcome is JobOutcome.error
            assert isinstance(result.exception, RuntimeError)
            assert str(result.exception) == "failing as requested"

    async def test_add_job_get_result_no_ready_yet(self) -> None:
        send, receive = create_memory_object_stream[Event](4)
        async with AsyncExitStack() as exit_stack:
            exit_stack.enter_context(send)
            exit_stack.enter_context(receive)
            scheduler = await exit_stack.enter_async_context(AsyncScheduler())
            scheduler.subscribe(send.send)
            job_id = await scheduler.add_job(dummy_async_job, kwargs={"delay": 0.2})

            with fail_after(3):
                event = await receive.receive()
                assert isinstance(event, TaskAdded)

                event = await receive.receive()
                assert isinstance(event, JobAdded)
                assert event.job_id == job_id

            with pytest.raises(JobLookupError), fail_after(1):
                await scheduler.get_job_result(job_id, wait=False)

    async def test_add_job_not_rewriting_task_config(
        self, raw_datastore: DataStore
    ) -> None:
        async with AsyncScheduler(data_store=raw_datastore) as scheduler:
            TASK_ID = "task_dummy_async_job"
            JOB_EXECUTOR = "async"
            MISFIRE_GRACE_TIME = timedelta(seconds=10)
            MAX_RUNNING_JOBS = 5
            METADATA = {"key": "value"}

            await scheduler.configure_task(
                func_or_task_id=TASK_ID,
                func=dummy_async_job,
                job_executor=JOB_EXECUTOR,
                misfire_grace_time=MISFIRE_GRACE_TIME,
                max_running_jobs=MAX_RUNNING_JOBS,
                metadata=METADATA,
            )

            assert await scheduler.add_job(TASK_ID)

            task = await scheduler.data_store.get_task(TASK_ID)
            assert task.job_executor == JOB_EXECUTOR
            assert task.misfire_grace_time == MISFIRE_GRACE_TIME
            assert task.max_running_jobs == MAX_RUNNING_JOBS
            assert task.metadata == METADATA

    async def test_contextvars(self, mocker: MockerFixture, timezone: ZoneInfo) -> None:
        def check_contextvars() -> None:
            assert current_async_scheduler.get() is scheduler
            info = current_job.get()
            assert isinstance(info, Job)
            assert info.task_id == "contextvars"
            assert info.schedule_id == "foo"
            assert info.original_scheduled_time == now
            assert info.scheduled_fire_time == now + timedelta(seconds=2.16)
            assert info.jitter == timedelta(seconds=2.16)
            assert info.start_deadline == now + timedelta(seconds=2.16) + timedelta(
                seconds=10
            )

        fake_uniform = mocker.patch("random.uniform")
        fake_uniform.configure_mock(return_value=2.16)
        now = datetime.now(timezone)
        send, receive = create_memory_object_stream[Event](1)
        async with AsyncExitStack() as exit_stack:
            exit_stack.enter_context(send)
            exit_stack.enter_context(receive)
            scheduler = await exit_stack.enter_async_context(AsyncScheduler())
            await scheduler.configure_task("contextvars", func=check_contextvars)
            await scheduler.add_schedule(
                "contextvars",
                DateTrigger(now),
                id="foo",
                max_jitter=3,
                misfire_grace_time=10,
            )
            scheduler.subscribe(send.send, {JobReleased})
            await scheduler.start_in_background()

            with fail_after(3):
                event = await receive.receive()

            assert event.outcome is JobOutcome.success

    async def test_explicit_cleanup(self, raw_datastore: DataStore) -> None:
        send, receive = create_memory_object_stream[Event](1)
        async with AsyncExitStack() as exit_stack:
            exit_stack.enter_context(send)
            exit_stack.enter_context(receive)
            scheduler = await exit_stack.enter_async_context(
                AsyncScheduler(raw_datastore, cleanup_interval=None)
            )
            scheduler.subscribe(send.send, {ScheduleRemoved})
            event = anyio.Event()
            scheduler.subscribe(lambda _: event.set(), {JobReleased}, one_shot=True)
            await scheduler.start_in_background()

            # Add a job whose result expires after 1 ms
            job_id = await scheduler.add_job(
                dummy_async_job, result_expiration_time=0.001
            )
            with fail_after(3):
                await event.wait()

            # After the sleeping past the expiration time and performing a cleanup, the
            # result should not be there anymore
            await sleep(0.1)
            await scheduler.cleanup()
            with pytest.raises(JobLookupError):
                await scheduler.get_job_result(job_id)

            # Add a schedule to immediately set the event
            event = anyio.Event()
            scheduler.subscribe(lambda _: event.set(), {JobReleased}, one_shot=True)
            await scheduler.add_schedule(
                dummy_async_job, DateTrigger(datetime.now(UTC)), id="event_set"
            )
            with fail_after(3):
                await event.wait()

            # The schedule should still be around, but with a null next_fire_time
            schedule = await scheduler.get_schedule("event_set")
            assert schedule.next_fire_time is None

            # After the cleanup, the schedule should be gone
            await scheduler.cleanup()
            with pytest.raises(ScheduleLookupError):
                await scheduler.get_schedule("event_set")

            # Check that the corresponding event was received
            with fail_after(3):
                event = await receive.receive()
                assert isinstance(event, ScheduleRemoved)
                assert event.schedule_id == schedule.id
                assert event.finished

    async def test_explicit_cleanup_avoid_schedules_still_having_jobs(
        self, raw_datastore: DataStore
    ) -> None:
        send, receive = create_memory_object_stream[Event](4)
        async with AsyncExitStack() as exit_stack:
            exit_stack.enter_context(send)
            exit_stack.enter_context(receive)
            scheduler = await exit_stack.enter_async_context(
                AsyncScheduler(raw_datastore, cleanup_interval=None)
            )
            scheduler.subscribe(send.send, {ScheduleUpdated, JobAdded, JobReleased})
            await scheduler.start_in_background()

            # Add a schedule to immediately set the event
            dummy_event = anyio.Event()
            await scheduler.configure_task("event_set", func=dummy_event.wait)
            schedule_id = await scheduler.add_schedule(
                "event_set", DateTrigger(datetime.now(UTC)), id="event_set"
            )

            # Wait for the job to be submitted
            event = await receive.receive()
            assert isinstance(event, JobAdded)
            assert event.schedule_id == schedule_id

            # Wait for the schedule to be updated
            event = await receive.receive()
            assert isinstance(event, ScheduleUpdated)
            assert event.schedule_id == schedule_id
            assert event.next_fire_time is None

            # Check that there is a job for the schedule
            jobs = await scheduler.get_jobs()
            assert len(jobs) == 1
            assert jobs[0].schedule_id == schedule_id

            # After the cleanup, the schedule should still be around, with a
            # null next_fire_time
            await scheduler.cleanup()
            schedule = await scheduler.get_schedule("event_set")
            assert schedule.next_fire_time is None

            # Wait for the job to finish
            dummy_event.set()
            event = await receive.receive()
            assert isinstance(event, JobReleased)

    async def test_implicit_cleanup(self, mocker: MockerFixture) -> None:
        """
        Test that the scheduler's cleanup() method is called when the scheduler is
        started.

        """
        async with AsyncScheduler() as scheduler:
            event = anyio.Event()
            mocker.patch.object(scheduler.data_store, "cleanup", side_effect=event.set)
            await scheduler.start_in_background()
            with fail_after(3):
                await event.wait()

    async def test_wait_until_stopped(self) -> None:
        async with AsyncScheduler() as scheduler:
            await scheduler.add_job(scheduler.stop)
            await scheduler.wait_until_stopped()

        # This should be a no-op
        await scheduler.wait_until_stopped()

    async def test_max_concurrent_jobs(self) -> None:
        lock = Lock()
        scheduler = AsyncScheduler(max_concurrent_jobs=1)
        tasks_done = 0

        async def acquire_release() -> None:
            nonlocal tasks_done
            lock.acquire_nowait()
            await sleep(0.1)
            tasks_done += 1
            if tasks_done == 2:
                await scheduler.stop()

            lock.release()

        with fail_after(3):
            async with scheduler:
                await scheduler.configure_task("dummyjob", func=acquire_release)
                await scheduler.add_job("dummyjob")
                await scheduler.add_job("dummyjob")
                await scheduler.run_until_stopped()

    @pytest.mark.parametrize(
        "trigger_type, run_job",
        [
            pytest.param("cron", False, id="cron"),
            pytest.param("date", True, id="date"),
        ],
    )
    async def test_pause_unpause_schedule(
        self,
        raw_datastore: DataStore,
        timezone: ZoneInfo,
        trigger_type: str,
        run_job: bool,
    ) -> None:
        if trigger_type == "cron":
            trigger = CronTrigger()
        else:
            trigger = DateTrigger(datetime.now(timezone))

        async with AsyncExitStack() as exit_stack:
            send, receive = create_memory_object_stream[Event](4)
            exit_stack.enter_context(send)
            exit_stack.enter_context(receive)
            scheduler = await exit_stack.enter_async_context(
                AsyncScheduler(data_store=raw_datastore, role=SchedulerRole.scheduler)
            )
            schedule_id = await scheduler.add_schedule(
                dummy_async_job, trigger, id="foo"
            )
            scheduler.subscribe(send.send, {ScheduleUpdated, JobAdded})

            # Pause the schedule and wait for the schedule update event
            await scheduler.pause_schedule(schedule_id)
            schedule = await scheduler.get_schedule(schedule_id)
            assert schedule.paused
            event = await receive.receive()
            assert isinstance(event, ScheduleUpdated)

            if run_job:
                # Make sure that no jobs are added when the scheduler is started
                await scheduler.start_in_background()
                assert not await scheduler.get_jobs()

            # Unpause the schedule and wait for the schedule update event
            await scheduler.unpause_schedule(schedule_id)
            schedule = await scheduler.get_schedule(schedule_id)
            assert not schedule.paused
            event = await receive.receive()
            assert isinstance(event, ScheduleUpdated)

            if run_job:
                with fail_after(3):
                    job_added_event = await receive.receive()

                assert isinstance(job_added_event, JobAdded)
                assert job_added_event.schedule_id == schedule_id

    async def test_schedule_job_result_expiration_time(
        self, raw_datastore: DataStore, timezone: ZoneInfo
    ) -> None:
        trigger = DateTrigger(datetime.now(timezone))
        send, receive = create_memory_object_stream[Event](4)
        with send, receive:
            async with AsyncExitStack() as exit_stack:
                scheduler = await exit_stack.enter_async_context(
                    AsyncScheduler(data_store=raw_datastore)
                )
                await scheduler.add_schedule(
                    dummy_async_job, trigger, id="foo", job_result_expiration_time=10
                )
                exit_stack.enter_context(scheduler.subscribe(send.send, {JobAdded}))
                await scheduler.start_in_background()

                # Wait for the scheduled job to be added
                with fail_after(3):
                    event = await receive.receive()
                    assert isinstance(event, JobAdded)
                    assert event.schedule_id == "foo"

                    # Get its result
                    result = await scheduler.get_job_result(event.job_id)

                assert result
                assert result.outcome is JobOutcome.success
                assert result.return_value == "returnvalue"

    async def test_scheduler_crash_restart_schedule_immediately(
        self, raw_datastore: DataStore, timezone: ZoneInfo
    ) -> None:
        """
        Test that the scheduler can immediately start processing a schedule it had
        acquired while the crash occurred.

        """
        scheduler = AsyncScheduler(data_store=raw_datastore)
        error_patch = patch.object(
            raw_datastore, "release_schedules", side_effect=RuntimeError("Fake failure")
        )
        with pytest.raises(ExceptionGroup) as exc_info, error_patch:
            async with scheduler:
                await scheduler.add_schedule(
                    dummy_async_job, IntervalTrigger(minutes=1), id="foo"
                )
                with move_on_after(3):
                    await scheduler.run_until_stopped()

                pytest.fail("The scheduler did not crash")

        exc = exc_info.value
        while isinstance(exc, ExceptionGroup) and len(exc.exceptions) == 1:
            exc = exc.exceptions[0]

        assert isinstance(exc, RuntimeError)
        assert exc.args == ("Fake failure",)

        # Don't clear the data store at launch
        if isinstance(raw_datastore, BaseExternalDataStore):
            raw_datastore.start_from_scratch = False

        # Now reinitialize the scheduler and make sure the schedule gets processed
        # immediately
        async with scheduler:
            # Check that the schedule was left in an acquired state
            schedules = await scheduler.get_schedules()
            assert len(schedules) == 1
            assert schedules[0].acquired_by == scheduler.identity
            assert schedules[0].acquired_until > datetime.now(timezone)

            # Start the scheduler and wait for the schedule to be processed
            await scheduler.start_in_background()
            with fail_after(scheduler.lease_duration.total_seconds() / 2):
                job_added_event = await scheduler.get_next_event(JobAdded)

            assert job_added_event.schedule_id == "foo"

    async def test_scheduler_crash_reap_abandoned_jobs(
        self, raw_datastore: DataStore, timezone: ZoneInfo
    ) -> None:
        """
        Test that after the scheduler has crashed and been restarted, it immediately
        detects an abandoned job and releases it with the appropriate result code.

        """
        scheduler = AsyncScheduler(data_store=raw_datastore)
        error_patch = patch.object(
            raw_datastore, "release_job", side_effect=RuntimeError("Fake failure")
        )
        with pytest.raises(ExceptionGroup) as exc_info, error_patch:
            async with scheduler:
                job_id = await scheduler.add_job(dummy_async_job)
                with move_on_after(3):
                    await scheduler.run_until_stopped()

                pytest.fail("The scheduler did not crash")

        exc = exc_info.value
        while isinstance(exc, ExceptionGroup) and len(exc.exceptions) == 1:
            exc = exc.exceptions[0]

        assert isinstance(exc, RuntimeError)
        assert exc.args == ("Fake failure",)

        # Don't clear the data store at launch
        if isinstance(raw_datastore, BaseExternalDataStore):
            raw_datastore.start_from_scratch = False

        # Now reinitialize the scheduler and make sure the job gets processed
        # immediately
        async with scheduler:
            # Check that the job was left in an acquired state
            jobs = await scheduler.get_jobs()
            assert len(jobs) == 1
            assert jobs[0].acquired_by == scheduler.identity
            assert jobs[0].acquired_until > datetime.now(timezone)

            trigger_event = anyio.Event()
            job_released_event: JobReleased | None = None

            def event_callback(event: Event) -> None:
                nonlocal job_released_event
                job_released_event = cast(JobReleased, event)
                trigger_event.set()

            # Start the scheduler and wait for the job to be processed
            with scheduler.subscribe(event_callback, {JobReleased}):
                await scheduler.start_in_background()
                with fail_after(scheduler.lease_duration.total_seconds() / 2):
                    await trigger_event.wait()

            assert job_released_event
            assert job_released_event.job_id == job_id
            assert job_released_event.outcome is JobOutcome.abandoned
            assert not await scheduler.get_jobs()

    async def test_stop_scheduler_while_job_running(
        self, raw_datastore: DataStore
    ) -> None:
        event = anyio.Event()

        async def delay_job() -> None:
            event.set()
            await sleep(8)

        async with AsyncScheduler(data_store=raw_datastore) as scheduler:
            await scheduler.configure_task("delay_job", func=delay_job)
            await scheduler.add_job("delay_job", result_expiration_time=15)
            await scheduler.start_in_background()

            with fail_after(5):
                await event.wait()

            assert len(scheduler._running_jobs) == 1
            job_id = next(iter(scheduler._running_jobs)).id
            event = anyio.Event()
            scheduler.subscribe(lambda _: event.set(), SchedulerStopped, one_shot=True)
            await scheduler.stop()
            with fail_after(5):
                await event.wait()

            assert len(scheduler._running_jobs) == 0

            # Check that the task has 0 running jobs
            datastore_tasks = await scheduler.get_tasks()
            assert len(datastore_tasks) == 1
            assert datastore_tasks[0].running_jobs == 0

            # Check that the job was removed
            datastore_jobs = await scheduler.get_jobs()
            assert len(datastore_jobs) == 0

            # Check that the job outcome was set to "cancelled"
            result = await scheduler.get_job_result(job_id)
            assert result.outcome is JobOutcome.cancelled


class TestSyncScheduler:
    def test_interface_parity(self) -> None:
        """
        Ensure that the sync scheduler has the same properties and methods as the async
        schedulers, and the method parameters match too.

        """
        actual_attributes = set(dir(Scheduler))
        expected_attributes = sorted(
            attrname for attrname in dir(AsyncScheduler) if not attrname.startswith("_")
        )
        for attrname in expected_attributes:
            if attrname not in actual_attributes:
                pytest.fail(f"SyncScheduler is missing the {attrname} attribute")

            async_attrval = getattr(AsyncScheduler, attrname)
            sync_attrval = getattr(Scheduler, attrname)
            if callable(async_attrval):
                async_sig = signature(async_attrval)
                async_args: dict[int, list] = defaultdict(list)
                for param in async_sig.parameters.values():
                    if param.name not in ("task_status", "is_async"):
                        async_args[param.kind].append(param)

                sync_sig = signature(sync_attrval)
                sync_args: dict[int, list] = defaultdict(list)
                for param in sync_sig.parameters.values():
                    sync_args[param.kind].append(param)

                for kind, args in async_args.items():
                    assert args == sync_args[kind], (
                        f"Parameter mismatch for {attrname}(): {args} != {sync_args[kind]}"
                    )

    def test_constructor_parity(self) -> None:
        """
        Ensure that the sync scheduler accepts the same constructor arguments as the
        async scheduler, with the same default values.

        The loop in :meth:`test_interface_parity` skips ``__init__`` because it only
        looks at public attributes, so the constructor defaults – the one place where
        the two schedulers can silently drift apart – are compared here instead.

        """
        # Constructor arguments whose declared defaults intentionally differ.
        # Scheduler only forwards the options it was actually given, so it uses None as
        # an "argument not passed" sentinel for every option whose real default is an
        # object built by AsyncScheduler itself (an attrs factory, or the module level
        # logger); the effective default still comes from AsyncScheduler.
        # task_defaults is additionally sync-specific, as Scheduler fills in the
        # "threadpool" job executor.
        exceptions = {
            "data_store",
            "event_broker",
            "job_executors",
            "task_defaults",
            "logger",
        }
        async_params = signature(AsyncScheduler.__init__).parameters
        sync_params = signature(Scheduler.__init__).parameters
        converters = {
            field.name: field.converter for field in attrs.fields(AsyncScheduler)
        }
        for name, async_param in async_params.items():
            if name == "self":
                continue

            if name not in sync_params:
                pytest.fail(f"Scheduler() is missing the {name!r} argument")

            if name in exceptions:
                continue

            # Compare the defaults the way AsyncScheduler stores them, by running both
            # through the field's converter (if any), as lease_duration is declared as a
            # plain number of seconds on the async side but as a timedelta on the sync
            # side.
            convert = converters[name] or (lambda value: value)
            async_default = convert(async_param.default)
            sync_default = convert(sync_params[name].default)
            assert sync_default == async_default, (
                f"Default value mismatch for the {name!r} argument of Scheduler(): "
                f"{sync_default!r} != {async_default!r}"
            )

    def test_repr(self) -> None:
        scheduler = Scheduler(identity="my identity")
        assert repr(scheduler) == (
            "Scheduler(identity='my identity', role=<SchedulerRole.both: 3>, "
            "data_store=MemoryDataStore(), event_broker=LocalEventBroker())"
        )

    def test_configure(self) -> None:
        executor = ThreadPoolJobExecutor()
        task_defaults = TaskDefaults(job_executor="executor1")
        scheduler = Scheduler(
            identity="identity",
            role=SchedulerRole.scheduler,
            max_concurrent_jobs=150,
            cleanup_interval=5,
            job_executors={"executor1": executor},
            task_defaults=task_defaults,
        )
        assert scheduler.identity == "identity"
        assert scheduler.role is SchedulerRole.scheduler
        assert scheduler.max_concurrent_jobs == 150
        assert scheduler.cleanup_interval == timedelta(seconds=5)
        assert scheduler.job_executors == {"executor1": executor}
        assert scheduler.task_defaults == task_defaults

    @pytest.mark.parametrize("as_default", [False, True])
    def test_threadpool_executor(self, as_default: bool) -> None:
        with Scheduler() as scheduler:
            scheduler.start_in_background()
            if as_default:
                thread_id = scheduler.run_job(threading.get_ident)
            else:
                thread_id = scheduler.run_job(
                    threading.get_ident, job_executor="threadpool"
                )

            assert thread_id != threading.get_ident()

    def test_processpool_executor(self) -> None:
        with Scheduler() as scheduler:
            scheduler.start_in_background()
            pid = scheduler.run_job(os.getpid, job_executor="processpool")
            assert pid != os.getpid()

    def test_properties(self) -> None:
        with Scheduler() as scheduler:
            assert isinstance(scheduler.data_store, MemoryDataStore)
            assert isinstance(scheduler.event_broker, LocalEventBroker)
            assert scheduler.role is SchedulerRole.both
            assert isinstance(scheduler.identity, str)
            assert len(scheduler.job_executors) == 3
            assert isinstance(scheduler.job_executors["async"], AsyncJobExecutor)
            assert isinstance(
                scheduler.job_executors["threadpool"], ThreadPoolJobExecutor
            )
            assert isinstance(
                scheduler.job_executors["processpool"], ProcessPoolJobExecutor
            )
            assert isinstance(scheduler.task_defaults, TaskDefaults)
            assert scheduler.state is RunState.stopped

    def test_use_without_contextmanager(self, mocker: MockFixture) -> None:
        fake_atexit_register = mocker.patch("atexit.register")
        scheduler = Scheduler()
        scheduler.subscribe(lambda event: None)
        fake_atexit_register.assert_called_once_with(scheduler._exit_stack.close)
        scheduler._exit_stack.close()

    def test_subscribe_coroutine_function(self) -> None:
        received = threading.Event()

        async def callback(event: Event) -> None:
            received.set()

        with Scheduler() as scheduler:
            scheduler.subscribe(callback, SchedulerStarted)
            scheduler.start_in_background()
            assert received.wait(10)

    def test_configure_task(self) -> None:
        queue: Queue[Event] = Queue()
        with Scheduler() as scheduler:
            scheduler.subscribe(queue.put_nowait)
            scheduler.configure_task("mytask", func=dummy_sync_job)
            scheduler.configure_task("mytask", misfire_grace_time=2)
            tasks = scheduler.get_tasks()
            assert len(tasks) == 1
            assert tasks[0].id == "mytask"
            assert tasks[0].func == f"{__name__}:dummy_sync_job"
            assert tasks[0].misfire_grace_time == timedelta(seconds=2)

        event = queue.get(timeout=1)
        assert isinstance(event, TaskAdded)
        assert event.task_id == "mytask"

        event = queue.get(timeout=1)
        assert isinstance(event, TaskUpdated)
        assert event.task_id == "mytask"

    def test_add_remove_schedule(self, timezone: ZoneInfo) -> None:
        queue: Queue[Event] = Queue()
        with Scheduler() as scheduler:
            scheduler.subscribe(queue.put_nowait)
            now = datetime.now(timezone)
            trigger = DateTrigger(now)
            schedule_id = scheduler.add_schedule(dummy_async_job, trigger, id="foo")
            assert schedule_id == "foo"

            schedules = scheduler.get_schedules()
            assert len(schedules) == 1
            assert schedules[0].id == "foo"
            assert schedules[0].task_id == f"{__name__}:dummy_async_job"

            schedule = scheduler.get_schedule(schedule_id)
            assert schedules[0] == schedule

            scheduler.remove_schedule(schedule_id)
            assert not scheduler.get_schedules()

        event = queue.get(timeout=1)
        assert isinstance(event, TaskAdded)
        assert event.task_id == f"{__name__}:dummy_async_job"

        event = queue.get(timeout=1)
        assert isinstance(event, ScheduleAdded)
        assert event.schedule_id == "foo"
        assert event.next_fire_time == now

        event = queue.get(timeout=1)
        assert isinstance(event, ScheduleRemoved)
        assert event.schedule_id == "foo"
        assert not event.finished

    def test_add_job_wait_result(self) -> None:
        queue: Queue[Event] = Queue()
        with Scheduler() as scheduler:
            assert scheduler.get_jobs() == []

            scheduler.subscribe(queue.put_nowait)
            job_id = scheduler.add_job(dummy_sync_job, result_expiration_time=10)

            event = queue.get(timeout=1)
            assert isinstance(event, TaskAdded)
            assert event.task_id == f"{__name__}:dummy_sync_job"

            event = queue.get(timeout=1)
            assert isinstance(event, JobAdded)
            assert event.job_id == job_id

            jobs = scheduler.get_jobs()
            assert len(jobs) == 1
            assert jobs[0].id == job_id
            assert jobs[0].task_id == f"{__name__}:dummy_sync_job"

            with pytest.raises(JobLookupError):
                scheduler.get_job_result(job_id, wait=False)

            scheduler.start_in_background()

            event = queue.get(timeout=1)
            assert isinstance(event, SchedulerStarted)

            event = queue.get(timeout=1)
            assert isinstance(event, JobAcquired)
            assert event.job_id == job_id
            assert event.task_id == f"{__name__}:dummy_sync_job"
            assert event.schedule_id is None

            event = queue.get(timeout=1)
            assert isinstance(event, JobReleased)
            assert event.job_id == job_id
            assert event.task_id == f"{__name__}:dummy_sync_job"
            assert event.schedule_id is None
            assert event.outcome is JobOutcome.success

            result = scheduler.get_job_result(job_id)
            assert result
            assert result.outcome is JobOutcome.success
            assert result.return_value == "returnvalue"

    def test_wait_until_stopped(self) -> None:
        queue: Queue[Event] = Queue()
        with Scheduler() as scheduler:
            scheduler.configure_task("stop", func=scheduler.stop)
            scheduler.add_job("stop")
            scheduler.subscribe(queue.put_nowait)
            scheduler.start_in_background()
            scheduler.wait_until_stopped()

        event = queue.get(timeout=1)
        assert isinstance(event, SchedulerStarted)

        event = queue.get(timeout=1)
        assert isinstance(event, JobAcquired)

        event = queue.get(timeout=1)
        assert isinstance(event, JobReleased)

        event = queue.get(timeout=1)
        assert isinstance(event, SchedulerStopped)

    def test_explicit_cleanup(self) -> None:
        with Scheduler(cleanup_interval=None) as scheduler:
            event = threading.Event()
            scheduler.add_schedule(
                event.set, DateTrigger(datetime.now(UTC)), id="event_set"
            )
            scheduler.start_in_background()
            assert event.wait(3)

            # The schedule should still be around, but with a null next_fire_time
            schedule = scheduler.get_schedule("event_set")
            assert schedule.next_fire_time is None

            # After the cleanup, the schedule should be gone
            scheduler.cleanup()
            with pytest.raises(ScheduleLookupError):
                scheduler.get_schedule("event_set")

    def test_implicit_cleanup(self, mocker: MockerFixture) -> None:
        """
        Test that the data store's cleanup() method is called when a scheduler
        configured with the default options is started.

        """
        with Scheduler() as scheduler:
            event = threading.Event()
            mocker.patch.object(scheduler.data_store, "cleanup", side_effect=event.set)
            scheduler.start_in_background()
            assert event.wait(3)

    def test_run_until_stopped(self) -> None:
        queue: Queue[Event] = Queue()
        with Scheduler() as scheduler:
            scheduler.configure_task("stop", func=scheduler.stop)
            scheduler.add_job("stop")
            scheduler.subscribe(queue.put_nowait)
            scheduler.run_until_stopped()

        event = queue.get(timeout=1)
        assert isinstance(event, SchedulerStarted)

        event = queue.get(timeout=1)
        assert isinstance(event, JobAcquired)

        event = queue.get(timeout=1)
        assert isinstance(event, JobReleased)

        event = queue.get(timeout=1)
        assert isinstance(event, SchedulerStopped)

    def test_uwsgi_threads_error(self, monkeypatch: MonkeyPatch) -> None:
        mod = ModuleType("uwsgi")
        monkeypatch.setitem(sys.modules, "uwsgi", mod)
        mod.has_threads = False
        with pytest.raises(
            RuntimeError, match="The scheduler seems to be running under uWSGI"
        ):
            Scheduler().start_in_background()
