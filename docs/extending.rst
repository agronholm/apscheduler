#####################
Extending APScheduler
#####################

This document is meant to explain how to develop your custom triggers, job stores, executors and schedulers.


Custom triggers
---------------

The built-in triggers cover the needs of the majority of all users.
However, some users may need specialized scheduling logic. To that end, the trigger system was made pluggable.

To implement your scheduling logic, subclass :class:`~apscheduler.triggers.base.BaseTrigger`.
Look at the interface documentation in that class. Then look at the existing trigger implementations.
That should give you a good idea what is expected of a trigger implementation.

To use your trigger, you can use :meth:`~apscheduler.schedulers.base.BaseScheduler.add_job` like this::

  trigger = MyTrigger(arg1='foo')
  scheduler.add_job(target, trigger)

You can also register it as a plugin so you can use can use the alternate form of ``add_jobstore``::

  scheduler.add_job(target, 'my_trigger', arg1='foo')

This is done by adding an entry point in your project's :file:`setup.py`:

.. code-block:: ini

  ...
  entry_points={
        'apscheduler.triggers': ['my_trigger = mytoppackage.subpackage:MyTrigger']
  }


Custom job stores
-----------------

If you want to store your jobs in a fancy new NoSQL database, or a totally custom datastore, you can implement your
own job store by subclassing :class:`~apscheduler.jobstores.base.BaseJobStore`.

A job store typically serializes the :class:`~apscheduler.job.Job` objects given to it, and constructs new Job objects
from binary data when they are loaded from the backing store. It is important that the job store restores the
``_scheduler`` and ``_jobstore_alias`` attribute of any Job that it creates. Refer to existing implementations for
examples.

It should be noted that :class:`~apscheduler.jobstores.memory.MemoryJobStore` is special in that it does not
deserialize the jobs. This comes with its own problems, which it handles in its own way.
If your job store does serialize jobs, you can of course use a serializer other than pickle.
You should, however, use the ``__getstate__`` and ``__setstate__`` special methods to respectively get and set the Job
state. Pickle uses them implicitly.

To use your job store, you can add it to the scheduler like this::

  jobstore = MyJobStore()
  scheduler.add_jobstore(jobstore, 'mystore')

You can also register it as a plugin so you can use can use the alternate form of ``add_jobstore``::

  scheduler.add_jobstore('my_jobstore', 'mystore')

This is done by adding an entry point in your project's :file:`setup.py`:

.. code-block:: ini

  ...
  entry_points={
        'apscheduler.jobstores': ['my_jobstore = mytoppackage.subpackage:MyJobStore']
  }


Custom executors
----------------

If you need custom logic for executing your jobs, you can create your own executor classes.
One scenario for this would be if you want to use distributed computing to run your jobs on other nodes.

Start by subclassing :class:`~apscheduler.executors.base.BaseExecutor`.
The responsibilities of an executor are as follows:
  * Performing any initialization when ``start()`` is called
  * Releasing any resources when ``shutdown()`` is called
  * Keeping track of the number of instances of each job running on it, and refusing to run more than the maximum
  * Notifying the scheduler of the results of the job

If your executor needs to serialize the jobs, make sure you either use pickle for it, or invoke the ``__getstate__`` and
``__setstate__`` special methods to respectively get and set the Job state. Pickle uses them implicitly.

To use your executor, you can add it to the scheduler like this::

  executor = MyExecutor()
  scheduler.add_executor(executor, 'myexecutor')

You can also register it as a plugin so you can use can use the alternate form of ``add_executor``::

  scheduler.add_executor('my_executor', 'myexecutor')

This is done by adding an entry point in your project's :file:`setup.py`:

.. code-block:: ini

  ...
  entry_points={
        'apscheduler.executors': ['my_executor = mytoppackage.subpackage:MyExecutor']
  }


Custom schedulers
-----------------

A typical situation where you would want to make your own scheduler subclass is when you want to integrate it with your
application framework of choice.

Your custom scheduler should always be a subclass of :class:`~apscheduler.schedulers.base.BaseScheduler`.
But if you're not adapting to a framework that relies on callbacks, consider subclassing
:class:`~apscheduler.schedulers.blocking.BlockingScheduler` instead.

The most typical extension points for scheduler subclasses are:
  * :meth:`~apscheduler.schedulers.base.BaseScheduler.start`
        must be overridden to wake up the scheduler for the first time
  * :meth:`~apscheduler.schedulers.base.BaseScheduler.shutdown`
        must be overridden to release resources allocated during ``start()``
  * :meth:`~apscheduler.schedulers.base.BaseScheduler.wakeup`
        must be overridden to manage the timernotify the scheduler of changes in the job store
  * :meth:`~apscheduler.schedulers.base.BaseScheduler._create_lock`
        override if your framework uses some alternate locking implementation (like gevent)
  * :meth:`~apscheduler.schedulers.base.BaseScheduler._create_default_executor`
        override if you need to use an alternative default executor

.. important:: Remember to call the superclass implementations of overridden methods, even abstract ones
   (unless they're empty).

The most important responsibility of the scheduler subclass is to manage the scheduler's sleeping based on the return
values of ``_process_jobs()``. This can be done in various ways, including setting timeouts in ``wakeup()`` or running
a blocking loop in ``start()``. Again, see the existing scheduler classes for examples.
