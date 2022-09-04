##########
User guide
##########


Installation
============

The preferred installation method is by using `pip <http://pypi.python.org/pypi/pip/>`_::

    $ pip install apscheduler

If you don't have pip installed, you need to
`install that first <https://pip.pypa.io/en/stable/installation/>`_.


Code examples
=============

The source distribution contains the :file:`examples` directory where you can find many
working examples for using APScheduler in different ways. The examples can also be
`browsed online <https://github.com/agronholm/apscheduler/tree/master/examples/?at=master>`_.


Introduction
============

The core concept of APScheduler is to give the user the ability to queue Python code to
be executed, either as soon as possible, later at a given time, or on a recurring
schedule. To make this happen, APScheduler has two types of components: *schedulers* and
*workers*.

A scheduler is the user-facing interface of the system. When running, it asks its
associated *data store* for *schedules* due to be run. For each such schedule, it then
uses the schedule's associated *trigger* to calculate run times up to the present. For
each run time, the scheduler creates a *job* in the data store, containing the
designated run time and the identifier of the schedule it was derived from.

A worker asks the data store for jobs, and then starts running those jobs. If the data
store signals that it has new jobs, the worker will try to acquire those jobs if it is
capable of accommodating more jobs. When a worker completes a job, it will then also ask
the data store for as many more jobs as it can handle.

By default, each scheduler starts an internal worker to simplify use, but in more
complex use cases you may wish to run them in separate processes, or even on separate
nodes. For this, you'll need both a persistent data store and an *event broker*, shared
by both the scheduler(s) and worker(s). For more information, see the section below on
running schedulers and workers separately.

Basic concepts / glossary
=========================

These are the basic components and concepts of APScheduler whixh will be referenced
later in this guide.

A *task* encapsulates a Python function and a number of configuration parameters. They
are often implicitly defined as a side effect of the user creating a new schedule
against a function, but can also be explicitly defined beforehand (**TODO**: implement
this!).

A *trigger* contains the logic and state used to calculate when a scheduled task should
be run.

A *schedule* combines a task with a trigger, plus a number of configuration parameters.

A *job* is request for a task to be run. It can be created automatically from a schedule
when a scheduler processes it, or it can be directly created by the user if they
directly request a task to be run.

A *data store* is used to store *schedules* and *jobs*, and to keep track of tasks.

A *scheduler* fetches schedules due for their next runs from its associated data store
and then creates new jobs accordingly.

A *worker* fetches jobs from its data store, runs them and pushes the results back to
the data store.

An *event broker* delivers published events to all interested parties. It facilitates
the cooperation between schedulers and workers by notifying them of new or updated
schedules or jobs.

Running the scheduler
=====================

The scheduler can run either in the foreground, blocking on a call to
:meth:`~apscheduler.schedulers.sync.Scheduler.run_until_complete`, or in the background
where it does its work while letting the rest of the program run.

If the only intent of your program is to run scheduled tasks, then you should start the
scheduler with :meth:`~apscheduler.schedulers.sync.Scheduler.run_until_complete`. But if
you need to do other things too, then you should call
:meth:`~apscheduler.schedulers.sync.Scheduler.start_in_background` before running the
rest of the program.

The scheduler can be used as a context manager. This initializes the underlying data
store and event broker, allowing you to use the scheduler for manipulating tasks and
schedules prior to actually starting it. Exiting the context manager will shut down
the scheduler and its underlying services. This mode of operation is mandatory for the
asynchronous scheduler when running it in the background, but it is preferred for the
synchronous scheduler too.

As a special consideration (for use with WSGI_ based web frameworks), the synchronous
scheduler can be run in the background without being used as a context manager. In this
scenario, the scheduler adds an :mod:`atexit` hook that will perform an orderly shutdown
of the scheduler before the process terminates.

.. _WSGI: https://wsgi.readthedocs.io/en/latest/what.html

.. warning:: If you start the scheduler in the background and let the script finish
   execution, the scheduler will automatically shut down as well.

.. tabs::

   .. code-tab:: python Synchronous (run in foreground)

      from apscheduler.schedulers.sync import Scheduler

      scheduler = Scheduler()
      # Add schedules, configure tasks here
      scheduler.run_until_stopped()

   .. code-tab:: python Synchronous (background thread; preferred method)

      from apscheduler.schedulers.sync import Scheduler

      with Scheduler() as scheduler:
          # Add schedules, configure tasks here
          scheduler.start_in_background()

   .. code-tab:: python Synchronous (background thread; WSGI alternative)

      from apscheduler.schedulers.sync import Scheduler

      scheduler = Scheduler()
      # Add schedules, configure tasks here
      scheduler.start_in_background()

   .. code-tab:: python Asynchronous (run in foreground)

      import asyncio

      from apscheduler.schedulers.async_ import AsyncScheduler

      async def main():
          async with AsyncScheduler() as scheduler:
              # Add schedules, configure tasks here
              await scheduler.run_until_stopped()

     asyncio.run(main())

   .. code-tab:: python Asynchronous (background task)

      import asyncio

      from apscheduler.schedulers.async_ import AsyncScheduler

      async def main():
          async with AsyncScheduler() as scheduler:
              # Add schedules, configure tasks here
              await scheduler.start_in_background()

     asyncio.run(main())

Scheduling tasks
================

To create a schedule for running a task, you need, at the minimum:

* A *callable* to be run
* A *trigger*

.. note:: Scheduling lambdas or nested functions is currently not possible. This will be
    fixed before the final release.

The callable can be a function or method, lambda or even an instance of a class that
contains the ``__call__()`` method. With the default (memory based) data store, any
callable can be used as a task callable. Persistent data stores (more on those below)
place some restrictions on the kinds of callables can be used because they cannot store
the callable directly but instead need to be able to locate it with a *reference*.

The trigger determines the scheduling logic for your schedule. In other words, it is
used to calculate the datetimes on which the task will be run. APScheduler comes with a
number of built-in trigger classes:

* :class:`~apscheduler.triggers.date.DateTrigger`:
  use when you want to run the task just once at a certain point of time
* :class:`~apscheduler.triggers.interval.IntervalTrigger`:
  use when you want to run the task at fixed intervals of time
* :class:`~apscheduler.triggers.cron.CronTrigger`:
  use when you want to run the task periodically at certain time(s) of day
* :class:`~apscheduler.triggers.calendarinterval.CalendarIntervalTrigger`:
  use when you want to run the task on calendar-based intervals, at a specific time of
  day

Combining multiple triggers
---------------------------

Occasionally, you may find yourself in a situation where your scheduling needs are too
complex to be handled with any of the built-in triggers directly.

One examples of such a need would be when you want the task to run at 10:00 from Monday
to Friday, but also at 11:00 from Saturday to Sunday.
A single :class:`~apscheduler.triggers.cron.CronTrigger` would not be able to handle
this case, but an :class:`~apscheduler.triggers.combining.OrTrigger` containing two cron
triggers can::

    from apscheduler.triggers.combining import OrTrigger
    from apscheduler.triggers.cron import CronTrigger

    trigger = OrTrigger(
        CronTrigger(day_of_week="mon-fri", hour=10),
        CronTrigger(day_of_week="sat-sun", hour=11),
    )

On the first run, :class:`~apscheduler.triggers.combining.OrTrigger` generates the next
run times from both cron triggers and saves them internally. It then returns the
earliest one. On the next run, it generates a new run time from the trigger that
produced the earliest run time on the previous run, and then again returns the earliest
of the two run times. This goes on until all the triggers have been exhausted, if ever.

Another example would be a case where you want the task to be run every 2 months at
10:00, but not on weekends (Saturday or Sunday)::

    from apscheduler.triggers.calendarinterval import CalendarIntervalTrigger
    from apscheduler.triggers.combining import AndTrigger
    from apscheduler.triggers.cron import CronTrigger

    trigger = AndTrigger(
        CalendarIntervalTrigger(months=2, hour=10),
        CronTrigger(day_of_week="mon-fri", hour=10),
    )

On the first run, :class:`~apscheduler.triggers.combining.AndTrigger` generates the next
run times from both the
:class:`~apscheduler.triggers.calendarinterval.CalendarIntervalTrigger` and
:class:`~apscheduler.triggers.cron.CronTrigger`. If the run times coincide, it will
return that run time. Otherwise, it will calculate a new run time from the trigger that
produced the earliest run time. It will keep doing this until a match is found, one of
the triggers has been exhausted or the maximum number of iterations (1000 by default) is
reached.

If this trigger is created on 2022-06-07 at 09:00:00, its first run times would be:

* 2022-06-07 10:00:00
* 2022-10-07 10:00:00
* 2022-12-07 10:00:00

Notably, 2022-08-07 is skipped because it falls on a Sunday.

Running tasks without scheduling
--------------------------------

In some cases, you want to run tasks directly, without involving schedules:

* You're only interested in using the scheduler system as a job queue
* You're interested in the job's return value

To queue a job and wait for its completion and get the result, the easiest way is to
use :meth:`~apscheduler.schedulers.sync.Scheduler.run_job`. If you prefer to just launch
a job and not wait for its result, use
:meth:`~apscheduler.schedulers.sync.Scheduler.add_job` instead. If you want to get the
results later, you can then call
:meth:`~apscheduler.schedulers.sync.Scheduler.get_job_result` with the job ID you got
from :meth:`~apscheduler.schedulers.sync.Scheduler.add_job`.

Removing schedules
------------------

To remove a previously added schedule, call
:meth:`~apscheduler.schedulers.sync.Scheduler.remove_schedule`. Pass the identifier of
the schedule you want to remove as an argument. This is the ID you got from
:meth:`~apscheduler.schedulers.sync.Scheduler.add_schedule`.

Note that removing a schedule does not cancel any jobs derived from it, but does prevent
further jobs from being created from that schedule.

Limiting the number of concurrently executing instances of a job
----------------------------------------------------------------

It is possible to control the maximum number of concurrently running jobs for a
particular task. By default, only one job is allowed to be run for every task.
This means that if the job is about to be run but there is another job for the same task
still running, the later job is terminated with the outcome of
:data:`~apscheduler.JobOutcome.missed_start_deadline`.

To allow more jobs to be concurrently running for a task, pass the desired maximum
number as the ``max_instances`` keyword argument to
:meth:`~apscheduler.schedulers.sync.Scheduler.add_schedule`.~

Controlling how much a job can be started late
----------------------------------------------

Some tasks are time sensitive, and should not be run at all if it fails to be started on
time (like, for example, if the worker(s) were down while they were supposed to be
running the scheduled jobs). You can control this time limit with the
``misfire_grace_time`` option passed to
:meth:`~apscheduler.schedulers.sync.Scheduler.add_schedule`. A worker that acquires the
job then checks if the current time is later than the deadline
(run time + misfire grace time) and if it is, it skips the execution of the job and
releases it with the outcome of :data:`~apscheduler.JobOutcome.`

Controlling how jobs are queued from schedules
----------------------------------------------

In most cases, when a scheduler processes a schedule, it queues a new job using the
run time currently marked for the schedule. Then it updates the next run time using the
schedule's trigger and releases the schedule back to the data store. But sometimes a
situation occurs where the schedule did not get processed often or quickly enough, and
one or more  next run times produced by the trigger are actually in the past.

In a situation like that, the scheduler needs to decide what to do: to queue a job for
every run time produced, or to *coalesce* them all into a single job, effectively just
kicking off a single job. To control this, pass the ``coalesce`` argument to
:meth:`~apscheduler.schedulers.sync.Scheduler.add_schedule`.

The possible values are:

* :data:`~apscheduler.CoalescePolicy.latest`: queue exactly one job, using the
  **latest** run time as the designated run time
* :data:`~apscheduler.CoalescePolicy.earliest`: queue exactly one job, using the
  **earliest** run time as the designated run time
* :data:`~apscheduler.CoalescePolicy.all`: queue one job for **each** of the calculated
  run times

The biggest difference between the first two options is how the designated run time, and
by extension, the starting deadline is for the job is selected. With the first option,
the job is less likely to be skipped due to being started late since the latest of all
the collected run times is used for the deadline calculation.

As explained in the previous section, the starting
deadline is *misfire grace time*
affects the newly queued job.

Context variables
=================

Schedulers and workers provide certain `context variables`_ available to the tasks being
run:

* The current scheduler: :data:`~apscheduler.current_scheduler`
* The current worker: :data:`~apscheduler.current_worker`
* Information about the job being currently run: :data:`~apscheduler.current_job`

Here's an example::

    from apscheduler import current_job

    def my_task_function():
        job_info = current_job.get().id
        print(
            f"This is job {job_info.id} and was spawned from schedule "
            f"{job_info.schedule_id}"
        )

.. _context variables: :mod:`contextvars`

.. _scheduler-events:

Subscribing to events
=====================

Schedulers and workers have the ability to notify listeners when some event occurs in
the scheduler system. Examples of such events would be schedulers or workers starting up
or shutting down, or schedules or jobs being created or removed from the data store.

To listen to events, you need a callable that takes a single positional argument which
is the event object. Then, you need to decide which events you're interested in:

.. tabs::

    .. code-tab:: python Synchronous

        from apscheduler import Event, JobAcquired, JobReleased

        def listener(event: Event) -> None:
            print(f"Received {event.__class__.__name__}")

        scheduler.events.subscribe(listener, {JobAcquired, JobReleased})

    .. code-tab:: python Asynchronous

        from apscheduler import Event, JobAcquired, JobReleased

        async def listener(event: Event) -> None:
            print(f"Received {event.__class__.__name__}")

        scheduler.events.subscribe(listener, {JobAcquired, JobReleased})

This example subscribes to the :class:`~apscheduler.JobAcquired` and
:class:`~apscheduler.JobAcquired` event types. The callback will receive an event of
either type, and prints the name of the class of the received event.

Asynchronous schedulers and workers support both synchronous and asynchronous callbacks,
but their synchronous counterparts only support synchronous callbacks.

When **distributed** event brokers (that is, other than the default one) are being used,
events other than the ones relating to the life cycles of schedulers and workers, will
be sent to all schedulers and workers connected to that event broker.

Deployment
==========

Using persistent data stores
----------------------------

The default data store, :class:`~apscheduler.datastores.memory.MemoryDataStore`, stores
data only in memory so all the schedules and jobs that were added to it will be erased
if the process crashes.

When you need your schedules and jobs to survive the application shutting down, you need
to use a *persistent data store*. Such data stores do have additional considerations,
compared to the memory data store:

* The task callable cannot be a lambda or a nested function
* Task arguments must be *serializable*
* You must either trust the data store, or use an alternate *serializer*
* A *conflict policy* and an *explicit identifier* must be defined for schedules that
  are added at application startup

These requirements warrant some explanation. The first point means that since persisting
data means saving it externally, either in a file or sending to a database server, all
the objects involved are converted to bytestrings. This process is called
*serialization*. By default, this is done using :mod:`pickle`, which guarantees the best
compatibility but is notorious for being vulnerable to simple injection attacks. This
brings us to the second point. If you cannot be sure that nobody can maliciously alter
the externally stored serialized data, it would be best to use another serializer. The
built-in alternatives are:

* :class:`~apscheduler.serializers.cbor.CBORSerializer`
* :class:`~apscheduler.serializers.json.JSONSerializer`

The former requires the cbor2_ library, but supports a wider variety of types natively.
The latter has no dependencies but has very limited support for different types.

The third point relates to situations where you're essentially adding the same schedule
to the data store over and over again. If you don't specify a static identifier for
the schedules added at the start of the application, you will end up with an increasing
number of redundant schedules doing the same thing, which is probably not what you want.
To that end, you will need to come up with some identifying name which will ensure that
the same schedule will not be added over and over again (as data stores are required to
enforce the uniqueness of schedule identifiers). You'll also need to decide what to do
if the schedule already exists in the data store (that is, when the application is
started the second time) by passing the ``conflict_policy`` argument. Usually you want
the :data:`~apscheduler.ConflictPolicy.replace` option, which replaces the existing
schedule with the new one.

.. seealso:: You can find practical examples of persistent data stores in the
    :file:`examples/standalone` directory (``async_postgres.py`` and
    ``async_mysql.py``).

.. _cbor2: https://pypi.org/project/cbor2/

Using multiple schedulers
-------------------------

There are several situations in which you would want to run several schedulers against
the same data store at once:

* Running a server application (usually a web app) with multiple workers
* You need fault tolerance (scheduling will continue even if a node or process running
  a scheduler goes down)

When you have multiple schedulers (or workers; see the next section) running at once,
they need to be able to coordinate their efforts so that the schedules don't get
processed more than once and the schedulers know when to wake up even if another
scheduler added the next due schedule to the data store. To this end, a shared
*event broker* must be configured.

.. seealso:: You can find practical examples of data store sharing in the
    :file:`examples/web` directory.

Running schedulers and workers separately
-----------------------------------------

Some deployment scenarios may warrant running workers separately from the schedulers.
For example, if you want to set up a scalable worker pool, you can run just the workers
in that pool and the schedulers elsewhere without the internal workers. To prevent the
scheduler from starting an internal worker, you need to pass it the
``start_worker=False`` option.

Starting a worker without a scheduler looks very similar to the procedure to start a
scheduler:

.. tabs::

    .. code-tab: python Synchronous

        from apscheduler.workers.sync import Worker


        data_store = ...
        event_broker = ...
        worker = Worker(data_store, event_broker)
        worker.run_until_stopped()

    .. code-tab: python asyncio

        import asyncio

        from apscheduler.workers.async_ import AsyncWorker


        async def main():
            data_store = ...
            event_broker = ...
            async with AsyncWorker(data_store, event_broker) as worker:
                await worker.wait_until_stopped()

        asyncio.run(main())

There is one significant matter to take into consideration if you do this. The scheduler
object, usually available from :data:`~apscheduler.current_scheduler`, will not be set
since there is no scheduler running in the current thread/task.

.. seealso:: A practical example of separate schedulers and workers can be found in the
    :file:`examples/separate_worker` directory.


.. _troubleshooting:

Troubleshooting
===============

If something isn't working as expected, it will be helpful to increase the logging level
of the ``apscheduler`` logger to the ``DEBUG`` level.

If you do not yet have logging enabled in the first place, you can do this::

    import logging

    logging.basicConfig()
    logging.getLogger('apscheduler').setLevel(logging.DEBUG)

This should provide lots of useful information about what's going on inside the
scheduler and/or worker.

Also make sure that you check the :doc:`faq` section to see if your problem already has
a solution.

Reporting bugs
==============

A `bug tracker <https://github.com/agronholm/apscheduler/issues>`_ is provided by
GitHub.

Getting help
============

If you have problems or other questions, you can either:

* Ask in the `apscheduler <https://gitter.im/apscheduler/Lobby>`_ room on Gitter
* Post a question on `GitHub discussions`_, or
* Post a question on StackOverflow_ and add the ``apscheduler`` tag

.. _GitHub discussions: https://github.com/agronholm/apscheduler/discussions/categories/q-a
.. _StackOverflow: http://stackoverflow.com/questions/tagged/apscheduler
