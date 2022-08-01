##########################
Frequently Asked Questions
##########################

Why doesn't the scheduler run my jobs?
======================================

This could be caused by a number of things. The two most common issues are:

#. Running the scheduler inside a uWSGI worker process while threads have not been
   enabled (see the next section for this)
#. Starting a synchronous scheduler with
   :meth:`~apscheduler.schedulers.sync.Scheduler.start_in_background` and then letting
   the execution reach the end of the script

To demonstrate the latter case, a script like this will **not work**::

    from apscheduler.schedulers.sync import Scheduler
    from apscheduler.schedulers.triggers.cron import CronTrigger


    def mytask():
        print("hello")

    scheduler = Scheduler()
    scheduler.start_in_background()
    scheduler.add_schedule(mytask, CronTrigger(hour=0))

The script above will **exit** right after calling
:meth:`~apscheduler.schedulers.sync.add_schedule` so the scheduler will not have a
chance to run the scheduled task.

If you're having any other issue, then enabling debug logging as instructed in the
:ref:`troubleshooting` section should shed some light into the problem.

Why am I getting a ValueError?
==============================

If you're receiving an error like the following::

   ValueError: This Job cannot be serialized since the reference to its callable (<bound method xxxxxxxx.on_crn_field_submission
   of <__main__.xxxxxxx object at xxxxxxxxxxxxx>>) could not be determined. Consider giving a textual reference (module:function
   name) instead.

This means that the function you are attempting to schedule has one of the following
problems:

* It is a lambda function (e.g. ``lambda x: x + 1``)
* It is a bound method (function tied to a particular instance of some class)
* It is a nested function (function inside another function)
* You are trying to schedule a function that is not tied to any actual module (such as a
  function defined in the REPL, hence ``__main__`` as the module name)

In these cases, it is impossible for the scheduler to determine a "lookup path" to find
that specific function instance in situations where, for example, the scheduler process
is restarted, or a process pool worker is being sent the related job object.

Common workarounds for these problems include:

* Converting a lambda to a regular function
* Moving a nested function to the module level or to class level as either a class
  method or a static method
* In case of a bound method, passing the unbound version (``YourClass.method_name``) as
  the target function to ``add_job()`` with the class instance as the first argument (so
  it gets passed as the ``self`` argument)

Is there a graphical user interface for APScheduler?
====================================================

No graphical interface is provided by the library itself. However, there are some third
party implementations, but APScheduler developers are not responsible for them. Here is
a potentially incomplete list:

* django_apscheduler_
* apschedulerweb_
* `Nextdoor scheduler`_

.. warning:: As of this writing, these third party offerings have not been updated to
    work with APScheduler 4.

.. _django_apscheduler: https://pypi.org/project/django-apscheduler/
.. _Flask-APScheduler: https://pypi.org/project/flask-apscheduler/
.. _pyramid_scheduler: https://github.com/cadithealth/pyramid_scheduler
.. _aiohttp: https://pypi.org/project/aiohttp/
.. _apschedulerweb: https://github.com/marwinxxii/apschedulerweb
.. _Nextdoor scheduler: https://github.com/Nextdoor/ndscheduler
