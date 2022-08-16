API reference
=============

Data structures
---------------

.. autoclass:: apscheduler.Task
.. autoclass:: apscheduler.Schedule
.. autoclass:: apscheduler.Job
.. autoclass:: apscheduler.JobInfo
.. autoclass:: apscheduler.JobResult
.. autoclass:: apscheduler.RetrySettings

Schedulers
----------

.. autoclass:: apscheduler.schedulers.sync.Scheduler
.. autoclass:: apscheduler.schedulers.async_.AsyncScheduler

Workers
-------

.. autoclass:: apscheduler.workers.sync.Worker
.. autoclass:: apscheduler.workers.async_.AsyncWorker

Data stores
-----------

.. autoclass:: apscheduler.abc.DataStore
.. autoclass:: apscheduler.abc.AsyncDataStore
.. autoclass:: apscheduler.datastores.memory.MemoryDataStore
.. autoclass:: apscheduler.datastores.sqlalchemy.SQLAlchemyDataStore
.. autoclass:: apscheduler.datastores.async_sqlalchemy.AsyncSQLAlchemyDataStore
.. autoclass:: apscheduler.datastores.mongodb.MongoDBDataStore

Event brokers
-------------

.. autoclass:: apscheduler.abc.EventBroker
.. autoclass:: apscheduler.abc.AsyncEventBroker
.. autoclass:: apscheduler.eventbrokers.local.LocalEventBroker
.. autoclass:: apscheduler.eventbrokers.async_local.LocalAsyncEventBroker
.. autoclass:: apscheduler.eventbrokers.asyncpg.AsyncpgEventBroker
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
.. autoclass:: apscheduler.WorkerEvent
.. autoclass:: apscheduler.WorkerStarted
.. autoclass:: apscheduler.WorkerStopped
.. autoclass:: apscheduler.JobAcquired
.. autoclass:: apscheduler.JobReleased

Enumerated types
----------------

.. autoclass:: apscheduler.RunState
.. autoclass:: apscheduler.JobOutcome
.. autoclass:: apscheduler.ConflictPolicy
.. autoclass:: apscheduler.CoalescePolicy

Context variables
-----------------

See the :mod:`contextvars` module for information on how to work with context variables.

.. data:: apscheduler.current_scheduler
   :annotation: the current scheduler
   :type: ~contextvars.ContextVar[~typing.Union[Scheduler, AsyncScheduler]]
.. data:: apscheduler.current_worker
   :annotation: the current scheduler
   :type: ~contextvars.ContextVar[~typing.Union[Worker, AsyncWorker]]
.. data:: apscheduler.current_job
   :annotation: information on the job being currently run
   :type: ~contextvars.ContextVar[JobInfo]

Exceptions
----------

.. autoexception:: apscheduler.TaskLookupError
.. autoexception:: apscheduler.ScheduleLookupError
.. autoexception:: apscheduler.JobLookupError
.. autoexception:: apscheduler.JobResultNotReady
.. autoexception:: apscheduler.JobCancelled
.. autoexception:: apscheduler.JobDeadlineMissed
.. autoexception:: apscheduler.ConflictingIdError
.. autoexception:: apscheduler.SerializationError
.. autoexception:: apscheduler.DeserializationError
.. autoexception:: apscheduler.MaxIterationsReached
