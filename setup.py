# coding: utf-8
try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup


setup(
    name='APScheduler',
    version='1.3.1',
    description='In-process task scheduler with Cron-like capabilities',
    long_description=open('README.txt').read(),
    author='Alex Gronholm',
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
    packages=['apscheduler'],
    zip_safe=True,
    test_suite='nose.collector',
    tests_require=['nose']
)
