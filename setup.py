# coding: utf-8
from setuptools import setup, find_packages


setup(
    name='APScheduler',
    version='1.01',
    description='In-process task scheduler with Cron-like capabilities',
    long_description="""\
APScheduler is a light but powerful in-process task scheduler that
lets you schedule functions (or any python callables) to be executed at times
of your choosing.

The development of APScheduler was heavily influenced by the `Quartz
<http://www.opensymphony.com/quartz/>`_ task scheduler written in Java,
although APScheduler cannot claim as many features.


Features
========

* No external dependencies
* Thread-safe API
* Cron-like scheduling
* Delayed scheduling of single fire jobs (like the UNIX "at" command)
* Interval-based scheduling of jobs, with configurable start date and
  repeat count


Documentation
=============

Documentation can be found on the `APScheduler site
<http://apscheduler.nextday.fi/>`_.


Source
======

The source can be browsed at `Bitbucket
<http://bitbucket.org/agronholm/apscheduler/src/>`_.
""",
    author=u'Alex Grönholm',
    author_email='apscheduler@nextday.fi',
    url='http://apscheduler.nextday.fi/',
    classifiers=[
      'Development Status :: 5 - Production/Stable',
      'Intended Audience :: Developers',
      'License :: OSI Approved :: MIT License',
      'Programming Language :: Python',
      'Programming Language :: Python :: 2.4',
      'Programming Language :: Python :: 2.5',
      'Programming Language :: Python :: 2.6',
      'Programming Language :: Python :: 3',
    ],
    keywords='scheduling cron',
    license='MIT',
    zip_safe=True,
    package_dir = {'': 'src'},
    packages=find_packages('src'),
    include_package_data=False,
    test_suite='nose.collector',
    tests_require = ['nose']
)
