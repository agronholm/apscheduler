import os.path

from setuptools import setup, find_packages


here = os.path.dirname(__file__)
readme_path = os.path.join(here, 'README.rst')
readme = open(readme_path).read()

setup(
    name='APScheduler',
    use_scm_version={
        'version_scheme': 'post-release',
        'local_scheme': 'dirty-tag'
    },
    description='In-process task scheduler with Cron-like capabilities',
    long_description=readme,
    author='Alex Grönholm',
    author_email='alex.gronholm@nextday.fi',
    url='https://github.com/agronholm/apscheduler',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3 :: Only',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7'
    ],
    keywords='scheduling cron',
    license='MIT',
    packages=find_packages(exclude=['tests']),
    python_requires='>= 3.5',
    setup_requires=[
        'setuptools_scm'
    ],
    install_requires=[
        'setuptools >= 0.7',
        'pytz',
        'tzlocal >= 1.2',
    ],
    extras_require={
        'gevent': ['gevent'],
        'mongodb': ['pymongo >= 2.8'],
        'redis': ['redis >= 3.0'],
        'rethinkdb': ['rethinkdb >= 2.4.0'],
        'sqlalchemy': ['sqlalchemy >= 0.8'],
        'tornado': ['tornado >= 4.3'],
        'twisted': ['twisted'],
        'zookeeper': ['kazoo'],
        'testing': [
            'pytest',
            'pytest-cov',
            'pytest-tornado5'
        ],
        'doc': [
            'sphinx',
            'sphinx-rtd-theme',
        ],
    },
    zip_safe=False,
    entry_points={
        'apscheduler.triggers': [
            'date = apscheduler.triggers.date:DateTrigger',
            'interval = apscheduler.triggers.interval:IntervalTrigger',
            'cron = apscheduler.triggers.cron:CronTrigger',
            'and = apscheduler.triggers.combining:AndTrigger',
            'or = apscheduler.triggers.combining:OrTrigger'
        ],
        'apscheduler.executors': [
            'debug = apscheduler.executors.debug:DebugExecutor',
            'threadpool = apscheduler.executors.pool:ThreadPoolExecutor',
            'processpool = apscheduler.executors.pool:ProcessPoolExecutor',
            'asyncio = apscheduler.executors.asyncio:AsyncIOExecutor',
            'gevent = apscheduler.executors.gevent:GeventExecutor [gevent]',
            'tornado = apscheduler.executors.tornado:TornadoExecutor [tornado]',
            'twisted = apscheduler.executors.twisted:TwistedExecutor [twisted]'
        ],
        'apscheduler.jobstores': [
            'memory = apscheduler.jobstores.memory:MemoryJobStore',
            'sqlalchemy = apscheduler.jobstores.sqlalchemy:SQLAlchemyJobStore [sqlalchemy]',
            'mongodb = apscheduler.jobstores.mongodb:MongoDBJobStore [mongodb]',
            'rethinkdb = apscheduler.jobstores.rethinkdb:RethinkDBJobStore [rethinkdb]',
            'redis = apscheduler.jobstores.redis:RedisJobStore [redis]',
            'zookeeper = apscheduler.jobstores.zookeeper:ZooKeeperJobStore [zookeeper]'
        ]
    }
)
