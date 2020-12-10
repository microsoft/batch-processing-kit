# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import cProfile
import multiprocessing
import signal
from multiprocessing.process import current_process
from threading import Thread
from time import sleep
from typing import List
import ctypes

from . import work_item
from .endpoint_status import EndpointStatusChecker
from .work_item import WorkItemRequest, WorkItemResult
from .logger import LogEventQueue
from .utils import kill_children_procs, NonDaemonicPool


class EndpointManager(Thread):
    def __init__(
            self, name: str, endpoint_name: str, endpoint_config: dict, log_folder: str, log_event_queue: LogEventQueue,
            cache_search_dirs: List[str], steal_work_fn, notify_work_success_fn, notify_work_failure_fn,
            endpoint_status_checker: EndpointStatusChecker, enable_profiling: bool = False):
        Thread.__init__(self, name="EndpointManager_{0}".format(name), daemon=True)
        self.name = name
        self.endpoint_name = endpoint_name
        self.endpoint_config = endpoint_config
        self.cache_search_dirs = cache_search_dirs
        self.logger = log_event_queue
        self.log_folder = log_folder
        self._steal_work_fn = steal_work_fn
        self._notify_work_success_fn = notify_work_success_fn
        self._notify_work_failure_fn = notify_work_failure_fn
        self._endpoint_status_checker: EndpointStatusChecker = endpoint_status_checker

        # Create Atomic Variables for RTF and Concurrency so that we can be
        # manipulated by our self or someone else and also consume the
        # current setting at any point. Initial value for RTF is as per
        # config. Concurrency after an endpoint is detected as being
        # online and healthy is set to config value by default.
        self.current_rtf = multiprocessing.Value(ctypes.c_float, float(self.endpoint_config['rtf']))
        self.current_concurrency = multiprocessing.Value(ctypes.c_int32, 0)  # Assume unhealthy til prove otherwise.

        # These are terminal once set for this EndpointManager.
        self._stop_requested = False
        self._cancellation_token = multiprocessing.Event()

        self._current_requests = 0
        self._current_requests_lock = multiprocessing.RLock()
        self._current_requests_cond = multiprocessing.Condition(self._current_requests_lock)
        self._successive_failures = 0
        self._cnt_apply_async = 0
        self._cnt_pool_cb = 0
        self._cnt_pool_cb_rets = 0
        self._in_steal_work_fn = False

        # cProfile.
        self._enable_profiling = enable_profiling
        if enable_profiling:
            self.pr = cProfile.Profile()

        # A process pool that will be used by this EndpointManager only.
        # Note that the concurrency Atomic Var and the proc pool are managed asynchronous to each other.
        assert multiprocessing.get_start_method() == 'fork'
        self._proc_pool: NonDaemonicPool = None

    def _init_proc_pool(self):
        """
        Intended for lazy initialization of the worker process pool by the EndpointManager main thread.
        This must be invoked before the very first work item could be run by this EndpointManager.
        """
        if not self._proc_pool:
            # Concurrency can change dynamically later but this is the starting pool size so that we are not
            # initially bottlenecked by the pool itself. Pool is later scaled up if concurrency exceeds it.
            start_pool_size = self.endpoint_config['concurrency']
            self.logger.debug("{0}: Lazy-initializing multiproc worker pool. Starting size: {1} procs".format(
                self.name, start_pool_size))

            def worker_entry(*args):
                current_process().name = NonDaemonicPool.sanitize_name(current_process().name, self.name)
                signal.signal(signal.SIGTERM, signal.SIG_DFL)
                signal.signal(signal.SIGINT, signal.SIG_DFL)
                work_item.init_proc_scope(self._cancellation_token, self.logger)
            self._proc_pool = NonDaemonicPool(start_pool_size, worker_entry)

    def set_endpoint_status_checker(self, endpoint_status_checker: EndpointStatusChecker):
        # Not necessary to update this functor under lock since we only ever read in this class.
        self._endpoint_status_checker = endpoint_status_checker

    def _finalize(self):
        self._cancellation_token.set()
        if self._enable_profiling:
            self.pr.disable()
            self.pr.dump_stats("/tmp/{0}".format(self.name))
        if self._proc_pool:
            self._proc_pool.close()
            self._proc_pool.terminate()
            self._proc_pool.join()

    def run(self):

        def check_throttle():
            while not self._stop_requested:
                try:
                    if not self._endpoint_status_checker.check_endpoint(
                            self.endpoint_config["host"],
                            self.endpoint_config["port"],
                            self.endpoint_config["isSecure"],
                            self.endpoint_config["isCloudService"]):
                        self.current_concurrency.value = 0
                        self.logger.warning("{0}: Endpoint {1}:{2} is unavailable at the moment "
                                            "so quarantining from requests.".format(
                                                self.name, self.endpoint_config["host"], self.endpoint_config["port"]))
                        sleep(20)
                    else:
                        self.current_concurrency.value = self.endpoint_config['concurrency']
                        with self._current_requests_lock:
                            self._current_requests_cond.notify()
                        sleep(3)
                    # Ensure the process pool size can always satisfy the current concurrency without queueing.
                    if self._proc_pool:
                        self._proc_pool.set_min_num_procs(self.current_concurrency.value)
                except Exception as e:
                    self.logger.error(
                        "EndpointManager {0} has its EndpointStatusChecker plug-in of type {1} failing "
                        "due to: {2}\n{3}".format(
                            self.name, type(self._endpoint_status_checker).__name__, e.__repr__(), e.__traceback__))

        Thread(
            target=check_throttle,
            name="{0}_BadEndpointThrottler".format(self.name),
            daemon=True
        ).start()

        if self._enable_profiling:
            self.pr.enable()

        # EndpointManager main loop.
        while True:
            # Wait until we have capacity (or woken up to stop).
            self._current_requests_lock.acquire()
            while True:
                if self._stop_requested:
                    self.logger.info("EndpointManager name: {0}  was requested to stop.".format(self.name))
                    self._cancellation_token.set()
                    self._current_requests_lock.release()
                    self._finalize()
                    return

                # Did we finally get capacity?
                if self._current_requests < self.current_concurrency.value:
                    break
                # Sleep until someone finishes something.
                self._current_requests_cond.wait()
            self._current_requests_lock.release()

            # Steal some work. This can also be returned prematurely
            # if we are being woken up to stop.
            self.logger.debug("EndpointManager name: {0}  will try to steal work".format(self.name))
            self._in_steal_work_fn = True  # No lock protection because only this loop can toggle.
            work: WorkItemRequest = self._steal_work_fn(self)
            self._in_steal_work_fn = False
            if work.filepath == "STOP":
                # This is an indicator we need to shut down.
                self.logger.info(
                    "EndpointManager name: {0}  was requested to stop while stealing work.".format(self.name))
                self._stop_requested = True
                self._cancellation_token.set()
                self._finalize()
                return

            self.logger.debug("EndpointManager name: {0}  stole work and will delegate to a worker.".format(self.name))
            # Assign the request to a worker.
            with self._current_requests_lock:
                self._current_requests += 1
                self._cnt_apply_async += 1

            self._init_proc_pool()  # Lazy init.
            self._proc_pool.apply_async(
                work.process,
                [self.endpoint_config, self.current_rtf.value],
                callback=self.pool_callback,
                error_callback=self.pool_error_callback
            )

    def request_stop(self):
        self._stop_requested = True
        self._cancellation_token.set()
        with self._current_requests_lock:  # python enforces notifier has sem, but unneeded by us
            self._current_requests_cond.notify()

    def pool_error_callback(self, exception: BaseException):
        # This is a path that should never be hit except developing,
        # but this count is in case we need to debug. As it is up to
        # WorkItemRequest to successfully return a WorkItemResult
        # (which could indicate success or failure outcome), this
        # should be invoked only when no WorkItemResult could be produced.
        msg = "EndpointManager {0} failure in a WorkItemRequest: {1}\n{2}".format(
            self.name, type(exception).__name__, exception.__traceback__)
        self.logger.fatal(msg)
        kill_children_procs()
        print(msg)  # Since may not get logged
        exit(1)

    def pool_callback(self, result: WorkItemResult):
        with self._current_requests_lock:
            self._cnt_pool_cb += 1

        if result.passed or result.cached:
            self._notify_work_success_fn(result.filepath, self, result)
            self._successive_failures = 0
        else:
            self._notify_work_failure_fn(result.filepath, self, result)
            with self._current_requests_lock:
                self._successive_failures += 1
                if self._successive_failures >= 5:
                    self.logger.critical(
                        "Endpoint manager {0} has failed {1} consecutive work items on endpoint {2}:{3}".format(
                            self.name, self._successive_failures,
                            self.endpoint_config["host"], self.endpoint_config["port"]
                        )
                    )
                    # Throttle back. Positive health check could restore.
                    self.current_concurrency.value = 0

        # As we just finished a request, we let the main thread
        # have an opportunity to check if we have capacity at this time
        # to steal and do another request.
        with self._current_requests_lock:
            self._current_requests -= 1
            self._current_requests_cond.notify()
            self._cnt_pool_cb_rets += 1
