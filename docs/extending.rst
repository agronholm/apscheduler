#####################
Extending APScheduler
#####################

.. py:currentmodule:: apscheduler

This document is meant to explain how to develop your custom triggers and data stores.

Custom triggers
---------------

The built-in triggers cover the needs of the majority of all users, particularly so when
combined using :class:`~triggers.combining.AndTrigger` and
:class:`~triggers.combining.OrTrigger`. However, some users may need specialized
scheduling logic. This can be accomplished by creating your own custom trigger class.

To implement your scheduling logic, create a new class that inherits from the
:class:`~abc.Trigger` interface class::

    from __future__ import annotations

    from apscheduler.abc import Trigger

    class MyCustomTrigger(Trigger):
        def next() -> datetime | None:
            ... # Your custom logic here

        def __getstate__():
            ... # Return the serializable state here

        def __setstate__(state):
            ... # Restore the state from the return value of __getstate__()

Requirements and constraints for trigger classes:

* :meth:`~abc.Trigger.next` must always either return a timezone aware
  :class:`~datetime.datetime` object or :data:`None` if a new run time cannot be
  calculated
* :meth:`~abc.Trigger.next` must never return the same :class:`~datetime.datetime`
  twice and never one that is earlier than the previously returned one
* :meth:`~abc.Trigger.__setstate__` must accept the return value of
  :meth:`~abc.Trigger.__getstate__` and restore the trigger to the functionally same
  state as the original
* :meth:`~abc.Trigger.__getstate__` may only return an object containing types
  serializable by :class:`~abc.Serializer`

Triggers are stateful objects. The :meth:`~abc.Trigger.next` method is where you
determine the next run time based on the current state of the trigger. The trigger's
internal state needs to be updated before returning to ensure that the trigger won't
return the same datetime on the next call. The trigger code does **not** need to be
thread-safe.

Custom job executors
--------------------

.. py:currentmodule:: apscheduler

If you need the ability to use third party frameworks or services to handle the
actual execution of jobs, you will need a custom job executor.

A job executor needs to inherit from :class:`~abc.JobExecutor`. This interface contains
one abstract method you're required to implement: :meth:`~abc.JobExecutor.run_job`.
This method is called with two arguments:

#. ``func``: the callable you're supposed to call
#. ``job``: the :class:`Job` instance

The :meth:`~abc.JobExecutor.run_job` implementation needs to call ``func`` with the
positional and keyword arguments attached to the job (``job.args`` and ``job.kwargs``,
respectively). The return value of the callable must be returned from the method.

Here's an example of a simple job executor that runs a (synchronous) callable in a
thread::

    from contextlib import AsyncExitStack
    from functools import partial

    from anyio import to_thread
    from apscheduler import Job
    from apscheduler.abc import JobExecutor

    class ThreadJobExecutor(JobExecutor):
        async def run_job(self, func: Callable[..., Any], job: Job) -> Any:
            wrapped = partial(func, *job.args, **job.kwargs)
            return await to_thread.run_sync(wrapped)

If you need to initialize some underlying services, you can override the
:meth:`~abc.JobExecutor.start` method. For example, the executor above could be improved
to take a maximum number of threads and create an AnyIO
:class:`~anyio.CapacityLimiter`::

    from contextlib import AsyncExitStack
    from functools import partial

    from anyio import CapacityLimiter, to_thread
    from apscheduler import Job
    from apscheduler.abc import JobExecutor

    class ThreadJobExecutor(JobExecutor):
        _limiter: CapacityLimiter

        def __init__(self, max_threads: int):
            self.max_threads = max_threads

        async def start(self, exit_stack: AsyncExitStack) -> None:
            self._limiter = CapacityLimiter(self.max_workers)

        async def run_job(self, func: Callable[..., Any], job: Job) -> Any:
            wrapped = partial(func, *job.args, **job.kwargs)
            return await to_thread.run_sync(wrapped, limiter=self._limiter)

Custom data stores
------------------

If you want to make use of some external service to store the scheduler data, and it's
not covered by a built-in data store implementation, you may want to create a custom
data store class.

A data store implementation needs to inherit from :class:`~abc.DataStore` and implement
several abstract methods:

* :meth:`~abc.DataStore.start`
* :meth:`~abc.DataStore.add_task`
* :meth:`~abc.DataStore.remove_task`
* :meth:`~abc.DataStore.get_task`
* :meth:`~abc.DataStore.get_tasks`
* :meth:`~abc.DataStore.add_schedule`
* :meth:`~abc.DataStore.remove_schedules`
* :meth:`~abc.DataStore.get_schedules`
* :meth:`~abc.DataStore.acquire_schedules`
* :meth:`~abc.DataStore.release_schedules`
* :meth:`~abc.DataStore.get_next_schedule_run_time`
* :meth:`~abc.DataStore.add_job`
* :meth:`~abc.DataStore.get_jobs`
* :meth:`~abc.DataStore.acquire_jobs`
* :meth:`~abc.DataStore.release_job`
* :meth:`~abc.DataStore.get_job_result`
* :meth:`~abc.DataStore.extend_acquired_schedule_leases`
* :meth:`~abc.DataStore.extend_acquired_job_leases`
* :meth:`~abc.DataStore.cleanup`

The :meth:`~abc.DataStore.start` method is where your implementation can perform any
initialization, including starting any background tasks. This method is called with two
arguments:

#. ``exit_stack``: an :class:`~contextlib.AsyncExitStack` object that can be used to
   work with context managers
#. ``event_broker``: the event broker that the store should be using to send events to
   other components of the system (including other schedulers)

The data store class needs to inherit from :class:`~abc.DataStore`::

    from contextlib import AsyncExitStack

    from apscheduler.abc import DataStore, EventBroker

    class MyCustomDataStore(DataStore):
        _event_broker: EventBroker

        async def start(self, exit_stack: AsyncExitStack, event_broker: EventBroker) -> None:
            # Save the event broker in a member attribute and initialize the store
            self._event_broker = event_broker

        # See the interface class for the rest of the abstract methods

Handling temporary failures
+++++++++++++++++++++++++++

If you plan to make your data store implementation public, it is strongly recommended
that you make an effort to ensure that the implementation can tolerate the loss of
connectivity to the backing store. The Tenacity_ library is used for this purpose by the
built-in stores to retry operations in case of a disconnection. If you use it to retry
operations when exceptions are raised, it is important to only do that in cases of
*temporary* errors, like connectivity loss, and not in cases like authentication
failure, missing database and so forth. See the built-in data store implementations and
Tenacity_ documentation for more information on how to pick the exceptions on which to
retry the operations.

.. _Tenacity: https://pypi.org/project/tenacity/
