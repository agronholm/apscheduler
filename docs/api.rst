API reference
=============

Data structures
---------------

.. autoclass:: apscheduler.Task
.. autoclass:: apscheduler.TaskDefaults
.. autoclass:: apscheduler.Schedule
.. autoclass:: apscheduler.ScheduleResult
.. autoclass:: apscheduler.Job
.. autoclass:: apscheduler.JobResult

Decorators
----------

.. autodecorator:: apscheduler.task

Schedulers
----------

.. autoclass:: apscheduler.Scheduler
.. autoclass:: apscheduler.AsyncScheduler

Job executors
-------------

.. autoclass:: apscheduler.abc.JobExecutor
.. autoclass:: apscheduler.executors.async_.AsyncJobExecutor
.. autoclass:: apscheduler.executors.subprocess.ProcessPoolJobExecutor
.. autoclass:: apscheduler.executors.qt.QtJobExecutor
.. autoclass:: apscheduler.executors.thread.ThreadPoolJobExecutor

Data stores
-----------

.. autoclass:: apscheduler.abc.DataStore
.. autoclass:: apscheduler.datastores.memory.MemoryDataStore
.. autoclass:: apscheduler.datastores.sqlalchemy.SQLAlchemyDataStore
.. autoclass:: apscheduler.datastores.mongodb.MongoDBDataStore

Event brokers
-------------

.. autoclass:: apscheduler.abc.EventBroker
.. autoclass:: apscheduler.abc.Subscription
.. autoclass:: apscheduler.eventbrokers.local.LocalEventBroker
.. autoclass:: apscheduler.eventbrokers.asyncpg.AsyncpgEventBroker
.. autoclass:: apscheduler.eventbrokers.psycopg.PsycopgEventBroker
.. autoclass:: apscheduler.eventbrokers.mqtt.MQTTEventBroker
.. autoclass:: apscheduler.eventbrokers.redis.RedisEventBroker

Serializers
-----------

.. autoclass:: apscheduler.abc.Serializer
.. autoclass:: apscheduler.serializers.cbor.CBORSerializer
.. autoclass:: apscheduler.serializers.json.JSONSerializer
.. autoclass:: apscheduler.serializers.pickle.PickleSerializer

Triggers
--------

.. autoclass:: apscheduler.abc.Trigger
   :special-members: __getstate__, __setstate__

.. autoclass:: apscheduler.triggers.date.DateTrigger
.. autoclass:: apscheduler.triggers.interval.IntervalTrigger
.. autoclass:: apscheduler.triggers.calendarinterval.CalendarIntervalTrigger
.. autoclass:: apscheduler.triggers.combining.AndTrigger
.. autoclass:: apscheduler.triggers.combining.OrTrigger
.. autoclass:: apscheduler.triggers.cron.CronTrigger

Events
------

.. autoclass:: apscheduler.Event
.. autoclass:: apscheduler.DataStoreEvent
.. autoclass:: apscheduler.TaskAdded
.. autoclass:: apscheduler.TaskUpdated
.. autoclass:: apscheduler.TaskRemoved
.. autoclass:: apscheduler.ScheduleAdded
.. autoclass:: apscheduler.ScheduleUpdated
.. autoclass:: apscheduler.ScheduleRemoved
.. autoclass:: apscheduler.JobAdded
.. autoclass:: apscheduler.JobRemoved
.. autoclass:: apscheduler.ScheduleDeserializationFailed
.. autoclass:: apscheduler.JobDeserializationFailed
.. autoclass:: apscheduler.SchedulerEvent
.. autoclass:: apscheduler.SchedulerStarted
.. autoclass:: apscheduler.SchedulerStopped
.. autoclass:: apscheduler.JobAcquired
.. autoclass:: apscheduler.JobReleased

Enumerated types
----------------

.. autoclass:: apscheduler.SchedulerRole()
    :show-inheritance:

.. autoclass:: apscheduler.RunState()
    :show-inheritance:

.. autoclass:: apscheduler.JobOutcome()
    :show-inheritance:

.. autoclass:: apscheduler.ConflictPolicy()
    :show-inheritance:

.. autoclass:: apscheduler.CoalescePolicy()
    :show-inheritance:

Context variables
-----------------

See the :mod:`contextvars` module for information on how to work with context variables.

.. data:: apscheduler.current_scheduler
   :type: ~contextvars.ContextVar[Scheduler]

   The current scheduler.

.. data:: apscheduler.current_async_scheduler
   :type: ~contextvars.ContextVar[AsyncScheduler]

   The current asynchronous scheduler.

.. data:: apscheduler.current_job
   :type: ~contextvars.ContextVar[Job]

   The job being currently run (available when running the job's target callable).

Exceptions
----------

.. autoexception:: apscheduler.TaskLookupError
.. autoexception:: apscheduler.ScheduleLookupError
.. autoexception:: apscheduler.JobLookupError
.. autoexception:: apscheduler.CallableLookupError
.. autoexception:: apscheduler.JobResultNotReady
.. autoexception:: apscheduler.JobCancelled
.. autoexception:: apscheduler.JobDeadlineMissed
.. autoexception:: apscheduler.ConflictingIdError
.. autoexception:: apscheduler.SerializationError
.. autoexception:: apscheduler.DeserializationError
.. autoexception:: apscheduler.MaxIterationsReached

Support classes for retrying failures
-------------------------------------

.. autoclass:: apscheduler.RetrySettings
.. autoclass:: apscheduler.RetryMixin

Support classes for unset options
---------------------------------

.. data:: apscheduler.unset

    Sentinel value for unset option values.

.. autoclass:: apscheduler.UnsetValue
