import unittest
from contextlib import redirect_stdout
from ctypes import cdll
import random
import os
import sys
import multiprocessing
from multiprocessing import Pipe, Value
import logging
from typing import Tuple, Any

from batchkit.utils import tee_to_pipe_decorator, NonDaemonicPool, FailedRecognitionError

"""
In this test module, there are primarily three things under test:
  1 -- tee_to_pipe_decorator
  2 -- NonDaemonicPool
  3 -- Our intended usage pattern of using them together when the pool worker proc
       uses a subproc and gets back return value and exception.
"""

logger = logging.getLogger("test_pool_tee")
logger.level = logging.DEBUG
log_stream_handler = logging.StreamHandler(sys.stdout)

# Toggle this to get useful debug trace.
# logger.addHandler(log_stream_handler)

test_root = os.path.dirname(os.path.realpath(__file__))
libblah_path = os.path.join(test_root, 'resources/libsegv.so')

lock = multiprocessing.Lock()
count_terms = Value('i', 0, lock=True)
count_exceptions = Value('i', 0, lock=True)
count_returns = Value('i', 0, lock=True)

# Emulates the work item on pool, run by the pool worker proc.
# Delegates the dangerous stuff to a subproc. We test the full
# pattern using tee_to_pipe_decorator() even though we may not
# have all of sig term, exception, and return in one app.
def parent_entry(id: int):
    global count_terms, count_exceptions, count_returns
    parent_conn, child_conn = Pipe()
    work_proc = multiprocessing.Process(
        target=tee_to_pipe_decorator(work_entry, child_conn),
        args=(id,))
    work_proc.start()

    _, status = os.waitpid(work_proc.pid, 0)
    if os.WIFSIGNALED(status):
        signum = os.WTERMSIG(status)
        assert signum == 11
        assert not parent_conn.poll()
        logger.debug("TERM")
        with count_terms.get_lock():
            count_terms.value += 1

    else:
        assert os.WIFEXITED(status)
        # We either have a return value or an exception
        assert parent_conn.poll()
        obj = parent_conn.recv()
        if isinstance(obj, Exception):
            logger.debug("EXCEPTION")
            with count_exceptions.get_lock():
                count_exceptions.value += 1
            # Making sure it's actually raisable else this pool proc dies
            # and we deadlock outside.
            try:
                raise obj
            except Exception as e:
                logger.debug("CAUGHT MYSELF: {0}".format(e))

        else:
            # This would fail if obj were not the successful return type.
            assert obj[0] == 123
            assert obj[1] == 456
            logger.debug("RETURN")
            with count_returns.get_lock():
                count_returns.value += 1

    parent_conn.close()
    child_conn.close()

    logger.debug("Parent {0} is returning".format(id))
    return None


def work_entry(somearg: int) -> Tuple[int, int]:
    if random.choice(["succeed", "segv"]) == "succeed":
        if random.choice(["succeed", "throw"]) == "succeed":
            return 123, 456
        else:
            raise FailedRecognitionError("a failed recognition")
    else:
        lib = cdll.LoadLibrary(libblah_path)
        lib.foo()
    return 123, 456


count_pool_success = 0
count_pool_errors = 0

def on_finish(anything: Any):
    global count_pool_success
    lock.acquire()
    count_pool_success += 1
    lock.release()

def on_error(anything: Any):
    global count_pool_errors
    lock.acquire()
    count_pool_errors += 1
    lock.release()

class TestPoolWithTee(unittest.TestCase):

    global count_pool_success, count_pool_errors
    global count_terms, count_exceptions, count_returns
    def test_NonDaemonicPool_with_tee_to_pipe(self):

        # Disable because we know it works and interfering due to parallel tests (need to prevent that).
        # return

        pool_procs = 4
        num_tasks = 100
        p = NonDaemonicPool(pool_procs)
        for i in range(num_tasks):
            p.apply_async(parent_entry, [i], callback=on_finish, error_callback=on_error)
        p.close()
        p.join()
        logger.debug("Final count_pool_success: {0}".format(count_pool_success))
        logger.debug("Final count_pool_errors: {0}".format(count_pool_errors))
        assert count_pool_success == num_tasks
        assert count_pool_errors == 0
        # Ensure we saw at least one of each.
        logger.debug("Final count_exceptions: {0}".format(count_exceptions.value))
        logger.debug("Final count_returns: {0}".format(count_returns.value))
        logger.debug("Final count_terms: {0}".format(count_terms.value))
        assert count_exceptions.value > 0
        assert count_returns.value > 0
        assert count_terms.value > 0
        assert count_exceptions.value + count_returns.value + count_terms.value == num_tasks


if __name__ == '__main__':
    multiprocessing.set_start_method('fork')
    unittest.main()