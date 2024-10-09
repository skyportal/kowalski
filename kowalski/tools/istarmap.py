# istarmap.py for Python 3.8+
# See https://stackoverflow.com/questions/57354700/starmap-combined-with-tqdm
import multiprocessing.pool as mpp
import sys
import numpy as np

# for numpy>=1.24.0, monkey patch numpy.object
np.object = object

if sys.version_info.minor > 7:

    def istarmap(self, func, iterable, chunksize=1):
        """starmap-version of imap"""
        self._check_running()
        if chunksize < 1:
            raise ValueError("Chunksize must be 1+, not {0:n}".format(chunksize))

        task_batches = mpp.Pool._get_tasks(func, iterable, chunksize)
        result = mpp.IMapIterator(self)
        self._taskqueue.put(
            (
                self._guarded_task_generation(
                    result._job, mpp.starmapstar, task_batches
                ),
                result._set_length,
            )
        )
        return (item for chunk in result for item in chunk)

else:

    def istarmap(self, func, iterable, chunksize=1):
        """starmap-version of imap"""
        if self._state != mpp.RUN:
            raise ValueError("Pool not running")

        if chunksize < 1:
            raise ValueError("Chunksize must be 1+, not {0:n}".format(chunksize))

        task_batches = mpp.Pool._get_tasks(func, iterable, chunksize)
        result = mpp.IMapIterator(self._cache)
        self._taskqueue.put(
            (
                self._guarded_task_generation(
                    result._job, mpp.starmapstar, task_batches
                ),
                result._set_length,
            )
        )
        return (item for chunk in result for item in chunk)


mpp.Pool.istarmap = istarmap
