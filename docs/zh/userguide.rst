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

可以通过一个简单的调用启动调度器 :meth:`~apscheduler.schedulers.base.BaseScheduler.start`
调度器除了 `~apscheduler.schedulers.blocking.BlockingScheduler`, 这个调用将立即执行，并且你可以继续初始化应用程序的进程将一个job添加到调度器。


对于 BlockingScheduler, 你只需要在完成初始化步骤以后调用 :meth:`~apscheduler.schedulers.base.BaseScheduler.start` 就可以了。

.. note:: 调度器启动以后，你就不必再修改这个设置了。


添加 job(作业)
-----------

这有两个办法将job添加到调度器:

#. 通过调用 :meth:`~apscheduler.schedulers.base.BaseScheduler.add_job`
#. 通过装饰器 :meth:`~apscheduler.schedulers.base.BaseScheduler.scheduled_job`

第一个方法是最普通的办法，第二个方法主要是为了方便声明一个不会在应用程序运行的过程当中改变的job。
:meth:`~apscheduler.schedulers.base.BaseScheduler.add_job` 方法返回一个
:class:`apscheduler.job.Job` 实例，这个实例可以在以后用来修改和删除job。

任何时候你都可以在调度器当中调度job。如果调度器还没有运行，当job被添加的时候，job将被暂时调度并且当调度器启动时第一次调度就被计算了。

如果你使用executor(执行器)或者 job store（作业存储区）初始化job的时候，你的job上需要两个必要条件。
It is important to note that if you use an executor or job store that serializes the job, it will add a couple
requirements on your job:

#. 目标调用必须是全局的
#. 任何参数的调用必须是可序列化的

内建job stores，只有MemoryJobStore 不序列化job。
内建executors，只有ProcessPoolExecutor 将序列化 job。


.. important:: 在程序初始化的期间，如果你的调度作业在一个持久化的作业存储区，你必须为你的job定义一个明确的ID并且使用``replace_existing=True``
   否则每次重新运行程序你都将得到一个新的job的复制。

.. tip:: 快速启动一个job可以在添加job的时候忽略触发器。


移除job（作业）
-------------

当你从调度器当中移除job的时候，这将移除相关的job store 并且它将不再被执行。
有两个方法移除job：

#. 通过job的ID和job store的别名调用 :meth:`~apscheduler.schedulers.base.BaseScheduler.remove_job`
#. 在job实例上调用 :meth:`~apscheduler.job.Job.remove` 这个实例是从
   :meth:`~apscheduler.schedulers.base.BaseScheduler.add_job` 得来的。

最后的方法也许更方便，但是它需要你存储在某处的 :class:`~apscheduler.job.Job` 实例，当添加job的时候你将接受它。
当job被调度且通过 :meth:`~apscheduler.schedulers.base.BaseScheduler.scheduled_job`, 第一个方法就是唯一的方法了。

如果job的调度结束（即，触发器不再生成进一步的运行时间），它将自动被移除。

Example::

    job = scheduler.add_job(myfunc, 'interval', minutes=2)
    job.remove()

同样的使用一个明确的 job ID::

    scheduler.add_job(myfunc, 'interval', minutes=2, id='my_job_id')
    scheduler.remove_job('my_job_id')


暂停和恢复 job（作业）
-------------------------

你可以很容易的通过一下方式暂停或者恢复作业 :class:`~apscheduler.job.Job` 实例或者调度器自己。

当job暂停的时候，下一次的运行次数将被清除并且没有进一步的运行次数将被计算，直到job被恢复。
暂停jbo使用下面的方法：

* :meth:`apscheduler.job.Job.pause`
* :meth:`apscheduler.schedulers.base.BaseScheduler.pause_job`

恢复:

* :meth:`apscheduler.job.Job.resume`
* :meth:`apscheduler.schedulers.base.BaseScheduler.resume_job`


获得被调度的 job的列表
--------------------------------

获得机器上被调度的job的进程列表，你可以使用 :meth:`~apscheduler.schedulers.base.BaseScheduler.get_jobs` 方法。
它将返回一个 :class:`~apscheduler.job.Job` 实例列表。如果你对想关注一个包含在job store当中的job感兴趣，你需要在第二个参数里面给提供一个job store的别名。

为了便利性你可以使用 :meth:`~apscheduler.schedulers.base.BaseScheduler.print_jobs` 方法，这个方法将打印出格式化的job列表，他们的触发器以及下一个运行次数。


修改 job
--------------

你可以修改任何job的参数通过调用 :meth:`apscheduler.job.Job.modify` 或者
:meth:`~apscheduler.schedulers.base.BaseScheduler.modify_job`. 你可以修改任何job参数除了”id“。

Example::

    job.modify(max_instances=6, name='Alternate name')

如果你想重新调度job，就是说改变它的触发器，你可以使用下面的方法
:meth:`apscheduler.job.Job.reschedule` 或者 :meth:`~apscheduler.schedulers.base.BaseScheduler.reschedule_job`。
这些方法为job构造一个新的触发器，并且基于新的触发器重新计算运行次数。

Example::

    scheduler.reschedule_job('my_job_id', trigger='cron', minute='*/5')


关闭调度器
---------------------------

关闭调度器::

    scheduler.shutdown()

默认情况下，等到所有正在执行的job都结束的时候，调度器会关闭它的job stores和executors。如果你不想等待你可以这样做::

    scheduler.shutdown(wait=False)

这将关闭job stores和executors但是不会等待任何运行中的任务完成。


暂停/恢复调度进程
-------------------------------

它可以暂定调度作业的进程::

    scheduler.pause()

暂停调度器直到进程恢复::

    scheduler.resume()

它可以启动暂停状态的调度器，就是说可以在还没有唤醒这个调度的时候调用它::

    scheduler.start(paused=True)

在运行之前当你需要删减不想要的job的时候这就显得格外有用的。


限制当前执行job实例的数量
----------------------------------------------------------------

默认情况下，只有一个实例的每一个工作被允许在同一时间执行。
这意味着如果job打算去运行但是之前的运行并没有结束，后面的运行就会失败。可以通过为一个特殊的job设置最大实例数，设置调度器最大可同时运行的job。
当添加job的时候使用 ``max_instances`` 关键词。


.. _missed-job-executions:

错过的工作执行以及合并
------------------------------------

有时调度程序可能无法在调度运行时执行调度作业。
最常见的情况是当作业被安排在持久性作业存储中并且调度程序被关闭并重新启动时，作业被意外执行。这种情况发生的时候，作业被认为是”失败的“。
调度器将根据作业的“misfire_grace_time”选项检查每个错过的执行次数（可以是在每个作业基础上设置或在调度程序中全局设置），以查看执行是否被触发。
这可以导致连续几次执行的工作。

如果这种行为对于特定用例是不被期望的，它可以使用 `coalescing` 去滚动所有的错过的执行。换句话说，如果为作业启用合并，并且调度程序看到一个或多个
排队执行的工作，它只会触发一次。绕过运行不会触失败事件。

.. note::
    如果执行一个作业被延迟，它是由于没有线程或者进程是有效的在进程池中，执行器也许会跳过它因为它延迟了（与原来指定的运行时间相比）。
    如果这个事情在你的应用程序当中，你可以在执行器当中增加线程或者进程数，或者调整``misfire_grace_time``设置到更高的数值。


.. _scheduler-events:

调度器事件
----------------

可以给调度器附加一个事件监听器。调度器事件在某些情况下被触发，并且可以在其中附加关于该特定事件的细节的附加信息。

通过给出适当的“mask”参数，可以只听特定类型的事件
:meth:`~apscheduler.schedulers.base.BaseScheduler.add_listener`将不同的常量组合在一起。
监听器可调用的参数是一个事件对象。

有关可用事件的详细信息，请参与一下文档 :mod:`~apscheduler.events` 模块。

例子::

    def my_listener(event):
        if event.exception:
            print('The job crashed :(')
        else:
            print('The job worked :)')

    scheduler.add_listener(my_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)


报告BUG
--------------

.. include:: ../README.rst
   :start-after: Reporting bugs
                 --------------
