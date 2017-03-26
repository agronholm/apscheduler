##########
用户指南
##########


安装 APScheduler
----------------------

最好的安装方式是通过 `pip <http://pypi.python.org/pypi/pip/>`_ 安装::

    $ pip install apscheduler

如果你没有安装pip，你可以下载和运行
`get-pip.py <https://bootstrap.pypa.io/get-pip.py>`_.

如果由于某些原因pip无法工作，你可以自己手动去PyPI
`下载 APScheduler
<https://pypi.python.org/pypi/APScheduler/>`_ ， 然后安装它::

    $ python setup.py install


代码例子
-------------

源码版本包含了 :file:`examples` 目录，在这里你可以找到很多不用的APScheduler的应用实例。代码例子也可以
`在线浏览 <https://github.com/agronholm/apscheduler/tree/master/examples/?at=master>`_.


基本概念
--------------

APScheduler 有4个组件:

* triggers
* job stores
* executors
* schedulers

*Triggers* 包含了作业的调度逻辑。每一个job(作业)都有它自己的trigger(触发器)用于决定接下来哪一个job(作业)会运行。
除了他们自己初始配置以外，触发器完全是无状态的。

*Job stores* 存储被调度的作业。默认的job store(作业存储区)把job(作业)存储在内存里，但是我们也可以把他们存储到各种数据库里。
当job(作业)的数据保存到持久性job stores(作业存储区)时，它将被序列化,当job(作业)被加载的时候会反序列化。
Job stores（除了默认的）不会将job(作业)的数据保存在内存当中，而是充当一个中间件在后台执行保存，载入，更新和搜索的任务。
Job stores不能被两个调度任务同时使用。

*Executors* 执行器是处理job(作业)运行的程序。他们通常通过在作业中提交制定的可调用对象到一个线程或者进程池来进行。
当job完成的时候，执行器会通知调度器。


*Schedulers* 调度器是将其它所有组件联系在一起的组件。在你的应用程序当中。你一般只有一个调度器
应用开发者通常不直接处理job stores(作业存储区)，executors(执行器)或者triggers(触发器)。相反，任务调度器提供适当的接口去处理所有的事情。
配置job stores和executors可以在调度器当中完成，比如添加，修改和删除job(作业)。


选择正确的 scheduler, job store(s), executor(s) 和 trigger(s)
----------------------------------------------------------------------

选择什么样的调度器取决于你程序的运行环境和你想使用APScheduler来做什么。
下面是一个选择调度器的快速开始指南：

* :class:`~apscheduler.schedulers.blocking.BlockingScheduler`:
  当调度器是您进程中唯一运行的程序的时候使用。
* :class:`~apscheduler.schedulers.background.BackgroundScheduler`:
  当你没有使用任何框架的情况下，并且你希望调度器在应用后台运行的时候使用。
* :class:`~apscheduler.schedulers.asyncio.AsyncIOScheduler`:
  当你的应用程序使用 asyncio 模块的时候使用。
* :class:`~apscheduler.schedulers.gevent.GeventScheduler`:
  当你的应用程序使用 gevent 的时候使用。
* :class:`~apscheduler.schedulers.tornado.TornadoScheduler`:
  当你正在构建一个Tornado程序的时候使用。
* :class:`~apscheduler.schedulers.twisted.TwistedScheduler`:
  当你正在构建 Twisted程序的时候使用。
* :class:`~apscheduler.schedulers.qt.QtScheduler`:
  当你正在构建QT应用程序的时候使用。

很简单对吧？

选择合适的job store的话，这取决于你是否需要job持久化。如果你总是在运行你的应用程序的时候重复创建你的job，你可以直接使用默认的job store。
(:class:`~apscheduler.jobstores.memory.MemoryJobStore`).如果你需要你的job在调度器重启或者应用崩溃的时候依然保持运行，这就要看你在的编程环境当中使用什么样的工具了。
我们推荐使用
:class:`~apscheduler.jobstores.sqlalchemy.SQLAlchemyJobStore` 链接一个 `PostgreSQL <http://www.postgresql.org/>`_ 这是因为它有极其强大的数据保护性。

同时，调度器的选择通常取决于你使用了什么样的框架。
另一方面，默认的 :class:`~apscheduler.executors.pool.ThreadPoolExecutor` 应该足够应付大多数的情况。
如果你的工作涉及到了CPU集群操作，你应该考虑使用
:class:`~apscheduler.executors.pool.ProcessPoolExecutor` 而不是去使用多个CPU核心。
当然你也可以两者同时使用，添加进程池作为备用的调度器。

当你调度一个job的的时候，你需要为它选择一个触发器。触发器决定了在某一个时间点上触发job的逻辑。APScheduler提供三个内建的触发器类型：

* :mod:`~apscheduler.triggers.date`:
  在job只在某一个时间点运行一次的情况下使用。
* :mod:`~apscheduler.triggers.interval`:
  job在固定时间间隔的情况下使用。
* :mod:`~apscheduler.triggers.cron`:
  job定期每天执行的情况下使用。

你可以找到每一个job store，executor和trigger类型的名字在他们的API文档页里面。


.. _scheduler-config:

配置调度器
-------------------------

APScheduler提供了许多不同的方式配置调度器。你可以使用一个配置词典或者关键词参数进行配置。你也可以先实例化调度器，添加job之后再配置调度器。
这样可以极大的兼容任何编程环境。

所有的调度器配置选项都可以在API参考
:class:`~apscheduler.schedulers.base.BaseScheduler` 类当中找到。调度器的子类也许有额外的选项，它们在它们各自的API参考当中。
job stores和executors的配置选项同样可以在API参考当中找到。

假如在程序里使用默认配置运行BackgroundScheduler ::

    from apscheduler.schedulers.background import BackgroundScheduler


    scheduler = BackgroundScheduler()

    # Initialize the rest of the application here, or before the scheduler initialization

这将给你一个BackgroundScheduler它带有一个名字叫做“default”的MemoryJobStore（内存作业存储区）和一个名字叫做“default”的ThreadPoolExecutor（线程池执行器）并且最大线程为10。

现在假设你向要更多，你向要有两个 job store 使用两个 executor并且你还想为新job调整默认值和设置一个不同的时区。
下面三个是三个类似的例子：

* 一个 MongoDBJobStore 名字为 "mongo"
* 一个 SQLAlchemyJobStore 名字为 "default" (使用 SQLite)
* 一个 ThreadPoolExecutor 名字为 "default", 20个线程
* 一个 ProcessPoolExecutor 名字为 "processpool", 5个线程
* UTC 作为调度器的时区
* 合并新job默认关闭。
* 默认最大的job实例限制为3个。

Method 1::

    from pytz import utc

    from apscheduler.schedulers.background import BackgroundScheduler
    from apscheduler.jobstores.mongodb import MongoDBJobStore
    from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
    from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor


    jobstores = {
        'mongo': MongoDBJobStore(),
        'default': SQLAlchemyJobStore(url='sqlite:///jobs.sqlite')
    }
    executors = {
        'default': ThreadPoolExecutor(20),
        'processpool': ProcessPoolExecutor(5)
    }
    job_defaults = {
        'coalesce': False,
        'max_instances': 3
    }
    scheduler = BackgroundScheduler(jobstores=jobstores, executors=executors, job_defaults=job_defaults, timezone=utc)

Method 2::

    from apscheduler.schedulers.background import BackgroundScheduler


    # The "apscheduler." prefix is hard coded
    scheduler = BackgroundScheduler({
        'apscheduler.jobstores.mongo': {
             'type': 'mongodb'
        },
        'apscheduler.jobstores.default': {
            'type': 'sqlalchemy',
            'url': 'sqlite:///jobs.sqlite'
        },
        'apscheduler.executors.default': {
            'class': 'apscheduler.executors.pool:ThreadPoolExecutor',
            'max_workers': '20'
        },
        'apscheduler.executors.processpool': {
            'type': 'processpool',
            'max_workers': '5'
        },
        'apscheduler.job_defaults.coalesce': 'false',
        'apscheduler.job_defaults.max_instances': '3',
        'apscheduler.timezone': 'UTC',
    })

Method 3::

    from pytz import utc

    from apscheduler.schedulers.background import BackgroundScheduler
    from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
    from apscheduler.executors.pool import ProcessPoolExecutor


    jobstores = {
        'mongo': {'type': 'mongodb'},
        'default': SQLAlchemyJobStore(url='sqlite:///jobs.sqlite')
    }
    executors = {
        'default': {'type': 'threadpool', 'max_workers': 20},
        'processpool': ProcessPoolExecutor(max_workers=5)
    }
    job_defaults = {
        'coalesce': False,
        'max_instances': 3
    }
    scheduler = BackgroundScheduler()

    # .. do something else here, maybe add jobs etc.

    scheduler.configure(jobstores=jobstores, executors=executors, job_defaults=job_defaults, timezone=utc)


启动调度器
----------------------

Starting the scheduler is done by simply calling :meth:`~apscheduler.schedulers.base.BaseScheduler.start` on the
scheduler. For schedulers other than `~apscheduler.schedulers.blocking.BlockingScheduler`, this call will return
immediately and you can continue the initialization process of your application, possibly adding jobs to the scheduler.

For BlockingScheduler, you will only want to call :meth:`~apscheduler.schedulers.base.BaseScheduler.start` after you're
done with any initialization steps.

.. note:: After the scheduler has been started, you can no longer alter its settings.


Adding jobs
-----------

There are two ways to add jobs to a scheduler:

#. by calling :meth:`~apscheduler.schedulers.base.BaseScheduler.add_job`
#. by decorating a function with :meth:`~apscheduler.schedulers.base.BaseScheduler.scheduled_job`

The first way is the most common way to do it. The second way is mostly a convenience to declare jobs that don't change
during the application's run time. The :meth:`~apscheduler.schedulers.base.BaseScheduler.add_job` method returns a
:class:`apscheduler.job.Job` instance that you can use to modify or remove the job later.

You can schedule jobs on the scheduler **at any time**. If the scheduler is not yet running when the job is added, the
job will be scheduled *tentatively* and its first run time will only be computed when the scheduler starts.

It is important to note that if you use an executor or job store that serializes the job, it will add a couple
requirements on your job:

#. The target callable must be globally accessible
#. Any arguments to the callable must be serializable

Of the builtin job stores, only MemoryJobStore doesn't serialize jobs.
Of the builtin executors, only ProcessPoolExecutor will serialize jobs.

.. important:: If you schedule jobs in a persistent job store during your application's initialization, you **MUST**
   define an explicit ID for the job and use ``replace_existing=True`` or you will get a new copy of the job every time
   your application restarts!

.. tip:: To run a job immediately, omit ``trigger`` argument when adding the job.


Removing jobs
-------------

When you remove a job from the scheduler, it is removed from its associated job store and will not be executed anymore.
There are two ways to make this happen:

#. by calling :meth:`~apscheduler.schedulers.base.BaseScheduler.remove_job` with the job's ID and job store alias
#. by calling :meth:`~apscheduler.job.Job.remove` on the Job instance you got from
   :meth:`~apscheduler.schedulers.base.BaseScheduler.add_job`

The latter method is probably more convenient, but it requires that you store somewhere the
:class:`~apscheduler.job.Job` instance you received when adding the job. For jobs scheduled via the
:meth:`~apscheduler.schedulers.base.BaseScheduler.scheduled_job`, the first way is the only way.

If the job's schedule ends (i.e. its trigger doesn't produce any further run times), it is automatically removed.

Example::

    job = scheduler.add_job(myfunc, 'interval', minutes=2)
    job.remove()

Same, using an explicit job ID::

    scheduler.add_job(myfunc, 'interval', minutes=2, id='my_job_id')
    scheduler.remove_job('my_job_id')


Pausing and resuming jobs
-------------------------

You can easily pause and resume jobs through either the :class:`~apscheduler.job.Job` instance or the scheduler itself.
When a job is paused, its next run time is cleared and no further run times will be calculated for it until the job is
resumed. To pause a job, use either method:

* :meth:`apscheduler.job.Job.pause`
* :meth:`apscheduler.schedulers.base.BaseScheduler.pause_job`

To resume:

* :meth:`apscheduler.job.Job.resume`
* :meth:`apscheduler.schedulers.base.BaseScheduler.resume_job`


Getting a list of scheduled jobs
--------------------------------

To get a machine processable list of the scheduled jobs, you can use the
:meth:`~apscheduler.schedulers.base.BaseScheduler.get_jobs` method. It will return a list of
:class:`~apscheduler.job.Job` instances. If you're only interested in the jobs contained in a particular job store,
then give a job store alias as the second argument.

As a convenience, you can use the :meth:`~apscheduler.schedulers.base.BaseScheduler.print_jobs` method which will print
out a formatted list of jobs, their triggers and next run times.


Modifying jobs
--------------

You can modify any job attributes by calling either :meth:`apscheduler.job.Job.modify` or
:meth:`~apscheduler.schedulers.base.BaseScheduler.modify_job`. You can modify any Job attributes except for ``id``.

Example::

    job.modify(max_instances=6, name='Alternate name')

If you want to reschedule the job -- that is, change its trigger, you can use either
:meth:`apscheduler.job.Job.reschedule` or :meth:`~apscheduler.schedulers.base.BaseScheduler.reschedule_job`.
These methods construct a new trigger for the job and recalculate its next run time based on the new trigger.

Example::

    scheduler.reschedule_job('my_job_id', trigger='cron', minute='*/5')


Shutting down the scheduler
---------------------------

To shut down the scheduler::

    scheduler.shutdown()

By default, the scheduler shuts down its job stores and executors and waits until all currently executing jobs are
finished. If you don't want to wait, you can do::

    scheduler.shutdown(wait=False)

This will still shut down the job stores and executors but does not wait for any running
tasks to complete.


Pausing/resuming job processing
-------------------------------

It is possible to pause the processing of scheduled jobs::

    scheduler.pause()

This will cause the scheduler to not wake up until processing is resumed::

    scheduler.resume()

It is also possible to start the scheduler in paused state, that is, without the first wakeup
call::

    scheduler.start(paused=True)

This is useful when you need to prune unwanted jobs before they have a chance to run.


Limiting the number of concurrently executing instances of a job
----------------------------------------------------------------

By default, only one instance of each job is allowed to be run at the same time.
This means that if the job is about to be run but the previous run hasn't finished yet, then the latest run is
considered a misfire. It is possible to set the maximum number of instances for a particular job that the scheduler will
let run concurrently, by using the ``max_instances`` keyword argument when adding the job.


.. _missed-job-executions:

Missed job executions and coalescing
------------------------------------

Sometimes the scheduler may be unable to execute a scheduled job at the time it was scheduled to run.
The most common case is when a job is scheduled in a persistent job store and the scheduler is shut down and restarted
after the job was supposed to execute. When this happens, the job is considered to have "misfired".
The scheduler will then check each missed execution time against the job's ``misfire_grace_time`` option (which can be
set on per-job basis or globally in the scheduler) to see if the execution should still be triggered.
This can lead into the job being executed several times in succession.

If this behavior is undesirable for your particular use case, it is possible to use `coalescing` to roll all these
missed executions into one. In other words, if coalescing is enabled for the job and the scheduler sees one or more
queued executions for the job, it will only trigger it once. No misfire events will be sent for the "bypassed" runs.

.. note::
    If the execution of a job is delayed due to no threads or processes being available in the pool, the executor may
    skip it due to it being run too late (compared to its originally designated run time).
    If this is likely to happen in your application, you may want to either increase the number of threads/processes in
    the executor, or adjust the ``misfire_grace_time`` setting to a higher value.


.. _scheduler-events:

Scheduler events
----------------

It is possible to attach event listeners to the scheduler. Scheduler events are fired on certain occasions, and may
carry additional information in them concerning the details of that particular event.
It is possible to listen to only particular types of events by giving the appropriate ``mask`` argument to
:meth:`~apscheduler.schedulers.base.BaseScheduler.add_listener`, OR'ing the different constants together.
The listener callable is called with one argument, the event object.

See the documentation for the :mod:`~apscheduler.events` module for specifics on the available events and their
attributes.

Example::

    def my_listener(event):
        if event.exception:
            print('The job crashed :(')
        else:
            print('The job worked :)')

    scheduler.add_listener(my_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)


Reporting bugs
--------------

.. include:: ../README.rst
   :start-after: Reporting bugs
                 --------------
