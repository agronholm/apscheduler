##########
User guide
##########

.. py:currentmodule:: apscheduler

Installation
============

The preferred installation method is by using
`pip <http://pypi.python.org/pypi/pip/>`_::

    $ pip install apscheduler

If you don't have pip installed, you need to
`install that first <https://pip.pypa.io/en/stable/installation/>`_.

Interfacing with certain external services may need extra dependencies which are
installable as extras:

* ``asyncpg``: for the AsyncPG event broker
* ``cbor``: for the CBOR serializer
* ``mongodb``: for the MongoDB data store
* ``mqtt``: for the MQTT event broker
* ``psycopg``: for the Psycopg event broker
* ``redis``: for the Redis event broker
* ``sqlalchemy``: for the SQLAlchemy data store

Using the extras instead of adding the corresponding libraries separately helps ensure
that you will have compatible versions of the dependent libraries going forward.

You can install any number of these extras with APScheduler by providing them as a comma
separated list inside the brackets, like this::

    pip install apscheduler[psycopg,sqlalchemy]

Code examples
=============

The source distribution contains the :file:`examples` directory where you can find many
working examples for using APScheduler in different ways. The examples can also be
`browsed online
<https://github.com/agronholm/apscheduler/tree/master/examples/?at=master>`_.


Introduction
============

The core concept of APScheduler is to give the user the ability to queue Python code to
be executed, either as soon as possible, later at a given time, or on a recurring
schedule.

The *scheduler* is the user-facing interface of the system. When it's running, it does
two things concurrently. The first is processing *schedules*. From its *data store*,
it fetches :ref:`schedules <schedule>` due to be run. For each such schedule, it then
uses the schedule's trigger_ to calculate run times up to the present. The scheduler
then creates one or more jobs (controllable by configuration) based on these run times
and adds them to the data store.

The second role of the scheduler is running :ref:`jobs <job>`. The scheduler asks the
`data store`_ for jobs, and then starts running those jobs. If the data store signals
that it has new jobs, the scheduler will try to acquire those jobs if it is capable of
accommodating more. When a scheduler completes a job, it will then also ask the data
store for as many more jobs as it can handle.

By default, schedulers operate in both of these roles, but can be configured to only
process schedules or run jobs if deemed necessary. It may even be desirable to use the
scheduler only as an interface to an external data store while leaving schedule and job
processing to other scheduler instances running elsewhere.

Basic concepts / glossary
=========================

These are the basic components and concepts of APScheduler which will be referenced
later in this guide.

.. _callable:

A *callable* is any object that returns ``True`` from :func:`callable`. These are:

* A free function (``def something(...): ...``)
* An instance method (``class Foo: ... def something(self, ...): ...``)
* A class method (``class Foo: ... @classmethod ... def something(cls, ...): ...``)
* A static method (``class Foo: ... @staticmethod ... def something(...): ...``)
* A lambda (``lambda a, b: a + b``)
* An instance of a class that contains a method named ``__call__``)

.. _task:

A *task* encapsulates a callable_ and a number of configuration parameters. They are
often implicitly defined as a side effect of the user creating a new schedule against a
callable_, but can also be :ref:`explicitly defined beforehand <configuring-tasks>`.

Tasks have three different roles:

#. They provide the target callable to be run when a job is started
#. They provide a key (task ID) on which to limit the maximum number of concurrent jobs,
   even between different schedules
#. They provide a template from which certain parameters, like job executor and misfire
   grace time, are copied to schedules and jobs derived from the task

.. _trigger:

A trigger_ contains the logic and state used to calculate when a scheduled task_ should
be run.

.. _schedule:

A *schedule* combines a task_ with a trigger_, plus a number of configuration
parameters.

.. _job:

A *job* is request for a task_ to be run. It can be created automatically from a
schedule when a scheduler processes it, or it can be directly created by the user if
they directly request a task_ to be run.

.. _data store:

A *data store* is used to store :ref:`schedules <schedule>` and :ref:`jobs <job>`, and
to keep track of :ref:`tasks <task>`.

.. _job executor:

A *job executor* runs the job_, by calling the function associated with the job's task.
An executor could directly call the callable_, or do it in another thread, subprocess or
even some external service.

.. _event broker:

An *event broker* delivers published events to all interested parties. It facilitates
the cooperation between schedulers by notifying them of new or updated
:ref:`schedules <schedule>` and :ref:`jobs <job>`.

.. _scheduler:

A *scheduler* is the main interface of this library. It houses both a `data store`_ and
an `event broker`_, plus one or more :ref:`job executors <job executor>`. It contains
methods users can use to work with tasks, schedules and jobs. Behind the scenes, it also
processes due schedules, spawning jobs and updating the next run times. It also
processes available jobs, making the appropriate :ref:`job executors <job executor>` to
run them, and then sending back the results to the `data store`_.

Running the scheduler
=====================

The scheduler_ comes in two flavors: synchronous and asynchronous. The synchronous
scheduler actually runs an asynchronous scheduler behind the scenes in a dedicated
thread, so if your app runs on :mod:`asyncio` or Trio_, you should prefer the
asynchronous scheduler.

The scheduler can run either in the foreground, blocking on a call to
:meth:`~Scheduler.run_until_stopped`, or in the background where it does its work while
letting the rest of the program run.

If the only intent of your program is to run scheduled tasks, then you should start the
scheduler with :meth:`~Scheduler.run_until_stopped`. But if you need to do other things
too, then you should call :meth:`~Scheduler.start_in_background` before running the rest
of the program.

In almost all cases, the scheduler should be used as a context manager. This initializes
the underlying `data store`_ and `event broker`_, allowing you to use the scheduler for
manipulating :ref:`tasks <task>`, :ref:`schedules <schedule>` and jobs prior to starting
the processing of schedules and jobs. Exiting the context manager will shut down the
scheduler and its underlying services. This mode of operation is mandatory for the
asynchronous scheduler when running it in the background, but it is preferred for the
synchronous scheduler too.

As a special consideration (for use with WSGI_ based web frameworks), the synchronous
scheduler can be run in the background without being used as a context manager. In this
scenario, the scheduler adds an :mod:`atexit` hook that will perform an orderly shutdown
of the scheduler before the process terminates.

.. _WSGI: https://wsgi.readthedocs.io/en/latest/what.html
.. _Trio: https://trio.readthedocs.io/en/stable/

.. warning:: If you start the scheduler in the background and let the script finish
   execution, the scheduler will automatically shut down as well.

.. tabs::

   .. code-tab:: python Synchronous (run in foreground)

      from apscheduler import Scheduler

      with Scheduler() as scheduler:
          # Add schedules, configure tasks here
          scheduler.run_until_stopped()

   .. code-tab:: python Synchronous (background thread; preferred method)

      from apscheduler import Scheduler

      with Scheduler() as scheduler:
          # Add schedules, configure tasks here
          scheduler.start_in_background()

   .. code-tab:: python Synchronous (background thread; WSGI alternative)

      from apscheduler import Scheduler

      scheduler = Scheduler()
      # Add schedules, configure tasks here
      scheduler.start_in_background()

   .. code-tab:: python Asynchronous (run in foreground)

      import asyncio

      from apscheduler import AsyncScheduler

      async def main():
          async with AsyncScheduler() as scheduler:
              # Add schedules, configure tasks here
              await scheduler.run_until_stopped()

     asyncio.run(main())

   .. code-tab:: python Asynchronous (background task)

      import asyncio

      from apscheduler import AsyncScheduler

      async def main():
          async with AsyncScheduler() as scheduler:
              # Add schedules, configure tasks here
              await scheduler.start_in_background()

     asyncio.run(main())

.. _configuring-tasks:

Configuring tasks
=================

In order to add :ref:`schedules <schedule>` or :ref:`jobs <job>` to the `data store`_,
you need to have a task_ that defines which callable_ will be called when each job_ is
run.

In most cases, you don't need to go through this step, and instead have a task_
implicitly created for you by the methods that add schedules or jobs.

Explicitly configuring a task is generally only necessary in the following cases:

* You need to have more than one task with the same callable
* You need to set any of the task settings to non-default values
* You need to add schedules/jobs targeting lambdas, nested functions or instances of
  unserializable classes

There are two ways to explicitly configure tasks:

#. Call the :meth:`~Scheduler.configure_task` scheduler method
#. Decorate your target function with :func:`@task <task>`

.. seealso:: :ref:`settings_inheritance`

Limiting the number of concurrently executing instances of a job
----------------------------------------------------------------

**Option**: ``max_running_jobs``

It is possible to control the maximum number of concurrently running jobs for a
particular task. By default, only one job is allowed to be run for every task.
This means that if the job is about to be run but there is another job for the same task
still running, the later job is terminated with the outcome of
:attr:`~JobOutcome.missed_start_deadline`.

To allow more jobs to be concurrently running for a task, pass the desired maximum
number as the ``max_running_jobs`` keyword argument to :meth:`~Scheduler.add_schedule`.

.. _controlling-how-much-a-job-can-be-started-late:

Controlling how much a job can be started late
----------------------------------------------

**Option**: ``misfire_grace_time``

This option applies to scheduled jobs. Some tasks are time sensitive, and should not be
run at all if they fail to be started on time (like, for example, if the scheduler(s)
were down while they were supposed to be running the scheduled jobs). When a scheduler
acquires jobs, the data store discards any jobs that have passed their start deadlines
(scheduled time + ``misfire_grace_time``). Such jobs are released with the outcome of
:attr:`~JobOutcome.missed_start_deadline`.

Adding custom metadata
----------------------

**Option**: ``metadata``

This option allows adding custom, JSON compatible metadata to tasks, schedules and jobs.
Here, "JSON compatible" means the following restrictions:

* The top-level metadata object must be a :class:`dict`
* All :class:`dict` keys must be strings
* Values can be :class:`int`, :class:`float`, :class:`str`, :class:`bool` or
  :data:`None`

.. note:: Top level metadata keys are merged with any explicitly passed values, in such
    a way that explicitly passed values override any values from the task level.

.. _settings_inheritance:

Inheritance of settings
-----------------------

When tasks are configured, or schedules or jobs created, they will inherit the settings
of any "parent" object according to the following rules:

* Task configuration parameters are resolving according to the following, descending
  priority order:

  #. Parameters passed directly to :meth:`~AsyncScheduler.configure_task`
  #. Parameters bound to the target function via :func:`@task <task>`
  #. The scheduler's task defaults
* Schedules inherit settings from the their respective tasks
* Jobs created from schedules inherit the settings from their parent schedules
* Jobs created directly inherit the settings from their parent tasks

The ``metadata`` parameter works a bit differently. Top level keys will be merged in
such a way that keys on a more explicit configuration level keys will overwrite keys
from a more generic level.

If any parameter is unset, it will be looked up on the next level. Here is an example
that illustrates the lookup order::

    from apscheduler import Scheduler, TaskDefaults, task

    @task(max_running_jobs=3, metadata={"foo": ["taskfunc"]})
    def mytaskfunc():
        print("running stuff")

    task_defaults = TaskDefaults(
        misfire_grace_time=15,
        job_executor="processpool",
        metadata={"global": 3, "foo": ["bar"]}
    )
    with Scheduler(task_defaults=task_defaults) as scheduler:
        scheduler.configure_task(
            "sometask",
            func=mytaskfunc,
            job_executor="threadpool",
            metadata={"direct": True}
        )

The resulting task will have the following parameters:

* ``id``: ``'sometask'`` (from the :meth:`~AsyncScheduler.configure_task` call)
* ``job_executor``: ``'threadpool'`` (from the :meth:`~AsyncScheduler.configure_task`
  call, where it overrides the scheduler-level default)
* ``max_running_jobs``: 3 (from the decorator)
* ``misfire_grace_time``: 15 (from the scheduler-level default)
* ``metadata``: ``{"global": 3, "foo": ["taskfunc"], "direct": True}``

Scheduling tasks
================

To create a schedule for running a task, you need, at the minimum:

* A preconfigured task_, OR a callable_ to be run
* A trigger_

If you've configured a task (as per the previous section), you can pass the task object
or its ID to :meth:`Scheduler.add_schedule`. As a shortcut, you can pass a callable_
instead, in which case a task will be automatically created for you if necessary.

If the callable you're trying to schedule is either a lambda or a nested function, then
you need to explicitly create a task beforehand, as it is not possible to create a
reference (``package.module:varname``) to these types of callables.

The trigger determines the scheduling logic for your schedule. In other words, it is
used to calculate the datetimes on which the task will be run. APScheduler comes with a
number of built-in trigger classes:

* :class:`~triggers.date.DateTrigger`:
  use when you want to run the task just once at a certain point of time
* :class:`~triggers.interval.IntervalTrigger`:
  use when you want to run the task at fixed intervals of time
* :class:`~triggers.cron.CronTrigger`:
  use when you want to run the task periodically at certain time(s) of day
* :class:`~triggers.calendarinterval.CalendarIntervalTrigger`:
  use when you want to run the task on calendar-based intervals, at a specific time of
  day

Combining multiple triggers
---------------------------

Occasionally, you may find yourself in a situation where your scheduling needs are too
complex to be handled with any of the built-in triggers directly.

One examples of such a need would be when you want the task to run at 10:00 from Monday
to Friday, but also at 11:00 from Saturday to Sunday.
A single :class:`~triggers.cron.CronTrigger` would not be able to handle
this case, but an :class:`~triggers.combining.OrTrigger` containing two cron
triggers can::

    from apscheduler.triggers.combining import OrTrigger
    from apscheduler.triggers.cron import CronTrigger

    trigger = OrTrigger(
        CronTrigger(day_of_week="mon-fri", hour=10),
        CronTrigger(day_of_week="sat-sun", hour=11),
    )

On the first run, :class:`~triggers.combining.OrTrigger` generates the next
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

On the first run, :class:`~triggers.combining.AndTrigger` generates the next
run times from both the
:class:`~triggers.calendarinterval.CalendarIntervalTrigger` and
:class:`~triggers.cron.CronTrigger`. If the run times coincide, it will
return that run time. Otherwise, it will calculate a new run time from the trigger that
produced the earliest run time. It will keep doing this until a match is found, one of
the triggers has been exhausted or the maximum number of iterations (1000 by default) is
reached.

If this trigger is created on 2022-06-07 at 09:00:00, its first run times would be:

* 2022-06-07 10:00:00
* 2022-10-07 10:00:00
* 2022-12-07 10:00:00

Notably, 2022-08-07 is skipped because it falls on a Sunday.

Removing schedules
------------------

To remove a previously added schedule, call
:meth:`~Scheduler.remove_schedule`. Pass the identifier of
the schedule you want to remove as an argument. This is the ID you got from
:meth:`~Scheduler.add_schedule`.

Note that removing a schedule does not cancel any jobs derived from it, but does prevent
further jobs from being created from that schedule.

Pausing schedules
-----------------

To pause a schedule, call :meth:`~Scheduler.pause_schedule`. Pass the identifier of the
schedule you want to pause as an argument. This is the ID you got from
:meth:`~Scheduler.add_schedule`.

Pausing a schedule prevents any new jobs from being created from it, but does not cancel
any jobs that have already been created from that schedule.

The schedule can be unpaused by calling :meth:`~Scheduler.unpause_schedule` with the
identifier of the schedule you want to unpause.

By default the schedule will retain the next fire time it had when it was paused, which
may result in the schedule being considered to have misfired when it is unpaused,
resulting in whatever misfire behavior it has configured
(see :ref:`controlling-how-much-a-job-can-be-started-late` for more details).

The ``resume_from`` parameter can be used to specify the time from which the schedule
should be resumed. This can be used to avoid the misfire behavior mentioned above. It
can be either a datetime object, or the string ``"now"`` as a convenient shorthand for
the current datetime. If this parameter is provided, the schedules trigger will be
repeatedly advanced to determine a next fire time that is at or after the specified time
to resume from.

Controlling how jobs are queued from schedules
----------------------------------------------

In most cases, when a scheduler processes a schedule, it queues a new job using the
run time currently marked for the schedule. Then it updates the next run time using the
schedule's trigger and releases the schedule back to the data store. But sometimes a
situation occurs where the schedule did not get processed often or quickly enough, and
one or more next run times produced by the trigger are actually in the past.

In a situation like that, the scheduler needs to decide what to do: to queue a job for
every run time produced, or to *coalesce* them all into a single job, effectively just
kicking off a single job. To control this, pass the ``coalesce`` argument to
:meth:`~Scheduler.add_schedule`.

The possible values are:

* :data:`~CoalescePolicy.latest`: queue exactly one job, using the
  **latest** run time as the designated run time
* :data:`~CoalescePolicy.earliest`: queue exactly one job, using the
  **earliest** run time as the designated run time
* :data:`~CoalescePolicy.all`: queue one job for **each** of the calculated
  run times

The biggest difference between the first two options is how the designated run time, and
by extension, the starting deadline for the job is selected. With the first option,
the job is less likely to be skipped due to being started late since the latest of all
the collected run times is used for the deadline calculation.

As explained in the previous section, the starting
deadline is *misfire grace time*
affects the newly queued job.

Running tasks without scheduling
================================

In some cases, you want to run tasks directly, without involving schedules:

* You're only interested in using the scheduler system as a job queue
* You're interested in the job's return value

To queue a job and wait for its completion and get the result, the easiest way is to
use :meth:`~Scheduler.run_job`. If you prefer to just launch a job and not wait for its
result, use :meth:`~Scheduler.add_job` instead. If you want to get the results later,
you need to pass an appropriate ``result_expiration_time`` parameter to
:meth:`~Scheduler.add_job` so that the result is saved. Then, you can call
:meth:`~Scheduler.get_job_result` with the job ID you got from
:meth:`~Scheduler.add_job` to retrieve the result.

Context variables
=================

Schedulers provide certain `context variables`_ available to the tasks being run:

* The current (synchronous) scheduler: :data:`~current_scheduler`
* The current asynchronous scheduler: :data:`~current_async_scheduler`
* Information about the job being currently run: :data:`~current_job`

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

Schedulers have the ability to notify listeners when some event occurs in the scheduler
system. Examples of such events would be schedulers or workers starting up or shutting
down, or schedules or jobs being created or removed from the data store.

To listen to events, you need a callable_ that takes a single positional argument
which is the event object. Then, you need to decide which events you're interested in:

.. tabs::

    .. code-tab:: python Synchronous

        from apscheduler import Event, JobAcquired, JobReleased

        def listener(event: Event) -> None:
            print(f"Received {event.__class__.__name__}")

        scheduler.subscribe(listener, {JobAcquired, JobReleased})

    .. code-tab:: python Asynchronous

        from apscheduler import Event, JobAcquired, JobReleased

        async def listener(event: Event) -> None:
            print(f"Received {event.__class__.__name__}")

        scheduler.subscribe(listener, {JobAcquired, JobReleased})

This example subscribes to the :class:`~JobAcquired` and
:class:`~JobReleased` event types. The callback will receive an event of
either type, and prints the name of the class of the received event.

Asynchronous schedulers and workers support both synchronous and asynchronous callbacks,
but their synchronous counterparts only support synchronous callbacks.

When **distributed** event brokers (that is, other than the default one) are being used,
events other than the ones relating to the life cycles of schedulers and workers, will
be sent to all schedulers and workers connected to that event broker.

Clean-up of expired jobs, job results and schedules
===================================================

Each scheduler runs the data store's :meth:`~.abc.DataStore.cleanup` method
periodically, configurable via the ``cleanup_interval`` scheduler parameter. This
ensures that the data store doesn't get filled with unused data over time.

Deployment
==========

Using persistent data stores
----------------------------

The default data store, :class:`~datastores.memory.MemoryDataStore`, stores
data only in memory so all the schedules and jobs that were added to it will be erased
if the process crashes.

When you need your schedules and jobs to survive the application shutting down, you need
to use a *persistent data store*. Such data stores do have additional considerations,
compared to the memory data store:

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

* :class:`~serializers.cbor.CBORSerializer`
* :class:`~serializers.json.JSONSerializer`

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
the :data:`~ConflictPolicy.replace` option, which replaces the existing
schedule with the new one.

.. seealso:: You can find practical examples of persistent data stores in the
    :file:`examples/standalone` directory (``async_postgres.py`` and
    ``async_mysql.py``).

.. _cbor2: https://pypi.org/project/cbor2/

Using multiple schedulers
-------------------------

There are several situations in which you would want to run several schedulers against
the same data store at once:

* Running a server application (usually a web app) with multiple worker processes
* You need fault tolerance (scheduling will continue even if a node or process running
  a scheduler goes down)

When you have multiple schedulers running at once, they need to be able to coordinate
their efforts so that the schedules don't get processed more than once and the
schedulers know when to wake up even if another scheduler added the next due schedule to
the data store. To this end, a shared *event broker* must be configured.

.. seealso:: You can find practical examples of data store sharing in the
    :file:`examples/web` directory.

Using a scheduler without running it
------------------------------------

Some deployment scenarios may warrant the use of a scheduler for only interfacing with
an external data store, for things like configuring tasks, adding schedules or queuing
jobs. One such practical use case is a web application that needs to run heavy
computations elsewhere so they don't cause performance issues with the web application
itself.

You can then run one or more schedulers against the same data store and event broker
elsewhere where they don't disturb the web application. These schedulers will do all the
heavy lifting like processing schedules and running jobs.

.. seealso:: A practical example of this separation of concerns can be found in the
    :file:`examples/separate_worker` directory.

Explicitly assigning an identity to the scheduler
-------------------------------------------------

If you're running one or more schedulers against a persistent data store in a production
setting, it'd be wise to assign each scheduler a custom identity. The reason for this is
twofold:

#. It helps you figure out which jobs are being run where
#. It allows crashed jobs to cleared out quicker, as other schedulers aren't allowed to
   clean them up until the jobs' timeouts expire

The best choice would be something that the environment guarantees to be unique among
all the scheduler instances but stays the same when the scheduler instance is restarted.
For example, on Kubernetes, this would be the name of the pod where the scheduler is
running, assuming of course that there is only one scheduler running in each pod against
the same data store.

Of course, if you're only ever running one scheduler against a persistent data store,
you can just use a static scheduler ID.

If no ID is explicitly given, the scheduler generates an ID by concatenating the
following:

* the current host name
* the current process ID
* the ID of the scheduler instance

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
