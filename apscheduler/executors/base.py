from abc import ABCMeta, abstractmethod

import six


class MaxInstancesReachedError(Exception):
    pass


class BaseExecutor(six.with_metaclass(ABCMeta, object)):
    """Base class of all executors."""

    @abstractmethod
    def submit_job(self, job, run_times):
        """
        Submits job for execution.

        :param job: job to execute
        :param run_times: list of `~datetime.datetime` objects specifying when the job should have been run
        :type job: `~apscheduler.scheduler.job.Job`
        :type run_times: list
        """

    def shutdown(self, wait=True):
        """
        Shuts down this executor.

        :param wait: ``True`` to wait until all submitted jobs have been executed
        """
