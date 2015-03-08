# coding: utf-8
import os.path
import sys

from setuptools import setup, find_packages
from setuptools.command.test import test as TestCommand

import apscheduler


class PyTest(TestCommand):
    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = []
        self.test_suite = True

    def run_tests(self):
        import pytest
        errno = pytest.main(self.test_args)
        sys.exit(errno)

extra_requirements = []
if sys.version_info < (3, 2):
    extra_requirements.append('futures')

here = os.path.dirname(__file__)
readme_path = os.path.join(here, 'README.rst')
readme = open(readme_path).read()

setup(
    name='APScheduler',
    version=apscheduler.release,
    description='In-process task scheduler with Cron-like capabilities',
    long_description=readme,
    author='Alex Gronholm',
    author_email='apscheduler@nextday.fi',
    url='http://pypi.python.org/pypi/APScheduler/',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.2',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4'
    ],
    keywords='scheduling cron',
    license='MIT',
    packages=find_packages(exclude=['tests']),
    install_requires=['setuptools >= 0.7', 'six >= 1.4.0', 'pytz', 'tzlocal'] + extra_requirements,
    tests_require=['pytest >= 2.5.1'],
    cmdclass={'test': PyTest},
    zip_safe=False,
    entry_points={
        'apscheduler.triggers': [
            'date = apscheduler.triggers.date:DateTrigger',
            'interval = apscheduler.triggers.interval:IntervalTrigger',
            'cron = apscheduler.triggers.cron:CronTrigger'
        ],
        'apscheduler.executors': [
            'debug = apscheduler.executors.debug:DebugExecutor',
            'threadpool = apscheduler.executors.pool:ThreadPoolExecutor',
            'processpool = apscheduler.executors.pool:ProcessPoolExecutor',
            'asyncio = apscheduler.executors.asyncio:AsyncIOExecutor',
            'gevent = apscheduler.executors.gevent:GeventExecutor',
            'twisted = apscheduler.executors.twisted:TwistedExecutor'
        ],
        'apscheduler.jobstores': [
            'memory = apscheduler.jobstores.memory:MemoryJobStore',
            'sqlalchemy = apscheduler.jobstores.sqlalchemy:SQLAlchemyJobStore',
            'mongodb = apscheduler.jobstores.mongodb:MongoDBJobStore',
            'redis = apscheduler.jobstores.redis:RedisJobStore'
        ]
    }
)
