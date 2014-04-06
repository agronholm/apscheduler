import concurrent.futures

from apscheduler.executors.base import BaseExecutor, run_job


class DebugExecutor(concurrent.futures.Executor):
    """A special executor that executes the target callable directly instead of deferring it to a thread or process."""

    def submit(self, fn, *args, **kwargs):
        f = concurrent.futures.Future()
        try:
            retval = fn(*args, **kwargs)
        except Exception as e:
            f.set_exception(e)
        else:
            f.set_result(retval)

        return f


class PoolExecutor(BaseExecutor):
    def __init__(self, pool_type, max_workers=10):
        super(PoolExecutor, self).__init__()

        if pool_type == 'thread':
            self._pool = concurrent.futures.ThreadPoolExecutor(max_workers)
        elif pool_type == 'process':
            self._pool = concurrent.futures.ProcessPoolExecutor(max_workers)
        elif pool_type == 'debug':
            self._pool = DebugExecutor()
        else:
            raise ValueError('Unknown pool type: %s' % pool_type)

    def _do_submit_job(self, job, run_times):
        f = self._pool.submit(run_job, job, run_times)
        callback = lambda f: self._run_job_success(job.id, f.result())
        f.add_done_callback(callback)

    def shutdown(self, wait=True):
        self._pool.shutdown(wait)
