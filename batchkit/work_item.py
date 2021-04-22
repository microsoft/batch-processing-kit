# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import copy
import multiprocessing
from multiprocessing import current_process, RLock
import os
import traceback
from abc import ABC, abstractmethod
from typing import Optional, List, Tuple
import jsonpickle
import heapq
import cProfile

from batchkit.logger import LogEventQueue, LogLevel

# Process-wide (work item pool worker) globals for CancellationToken, LogEventQueue, WorkItemLock_GlobalScope.
# These are objects that may not be serializable and are needed by all work items that a pool worker processes.
proc_scope_ct: Optional[multiprocessing.Event] = None
proc_scope_leq: Optional[LogEventQueue] = None
proc_scope_global_workitem_lock: Optional[RLock] = None


def init_proc_scope(
        cancellation_token: multiprocessing.Event, log_event_queue: LogEventQueue,
        global_workitem_lock: RLock):
    global proc_scope_ct, proc_scope_leq, proc_scope_global_workitem_lock
    proc_scope_ct = cancellation_token
    proc_scope_leq = log_event_queue
    proc_scope_global_workitem_lock = global_workitem_lock


class WorkItemRequest(ABC):
    def __init__(self, filepath: str, language: Optional[str] = None):
        self.filepath: str = filepath
        self.language: Optional[str] = language

    def serialize_json(self):
        return jsonpickle.encode(self)

    def process(self, endpoint_config: dict, rtf: float,
                enable_profiling: bool = False,

                # Work-item specifics do not need to be provided if they can be defaulted
                # from process scope via init_proc_scope().
                log_event_queue: Optional[LogEventQueue] = None,
                cancellation_token: Optional[multiprocessing.Event] = None,
                global_workitem_lock: Optional[RLock] = None):

        if enable_profiling:
            pr = cProfile.Profile()
            pr.enable()

        leq: LogEventQueue = log_event_queue if log_event_queue else proc_scope_leq
        ct: multiprocessing.Event = cancellation_token if cancellation_token else proc_scope_ct
        gwil: RLock = global_workitem_lock if global_workitem_lock else proc_scope_global_workitem_lock

        leq.debug("Process: {0} starting to process work item of Type: {1}  and Filepath: {2}".format(
            current_process().name, type(self).__name__, self.filepath))
        try:
            result: WorkItemResult = self.process_impl(endpoint_config, rtf, leq, ct, gwil)
        except Exception as err:
            tb = traceback.format_exc()
            leq.debug("{0}: Exception in WorkItemRequest.process(): {1}\n{2}".format(
                type(self).__name__, type(err).__name__, tb))
            raise err
        leq.debug("Process: {0} finished processing work item of Type: {1}  and Filepath: {2}".format(
            current_process().name, type(self).__name__, self.filepath))

        if enable_profiling:
            pr.disable()
            pr.dump_stats("/tmp/{0}_profile".format(os.path.basename(self.filepath)))
        return result

    @abstractmethod
    def process_impl(self, endpoint_config: dict, rtf: float,
                     log_event_queue: LogEventQueue, cancellation_token: multiprocessing.Event,
                     global_workitem_lock: RLock):
        pass

    def priority(self) -> int:   # noqa; intended virtual override
        """
        Higher value means higher priority. Default implementation returns
        the same value 0 for all items. Override to control ordering in which
        work items are processed first. The policy is that higher priority work items
        will be processed by the framework first. Items requiring more processing work
        should be given higher priority to reduce the laggard tail problem (greedy scheduling
        heuristic is used for work stealing).

        A value of -1 should be returned when there is an error attempting to determine
        priority since it is assumed these files would also fail-fast as work items
        and thus complete fastest. All negative priorities are treated in this way.
        """
        return 0


class SentinelWorkItemRequest(WorkItemRequest):
    def __init__(self):
        super().__init__("STOP", "")

    def process_impl(self, endpoint_config: dict, rtf: float,
                     log_event_queue: LogEventQueue, cancellation_token: multiprocessing.Event,
                     global_workitem_lock: RLock):
        return None


class WorkItemQueue:
    """
    Non-thread-safe priority queue.
    """
    def __init__(self, logger: LogEventQueue):
        self._arr: List[Tuple[int, WorkItemRequest]] = []
        self.logger = logger

    def put(self, item: WorkItemRequest):
        pri = item.priority()
        if pri > -1:
            self.logger.debug("Prioritizing work item: {0} at priority: {1}".format(item.filepath, pri))
        else:
            pri = -1  # All negative priorities are treated equally (unknown priority).
            self.logger.warning(
                "WorkItemQueue: Unable to determine priority for item: {0}. "
                "Lowest priority presumed.".format(item.filepath))

        heapq.heappush(self._arr, (-1 * pri, id(item), item))  # max priority queue from min heap

    def get(self) -> WorkItemRequest:
        return (heapq.heappop(self._arr))[2]


class WorkItemResult(ABC):
    def __init__(self,
                 request: WorkItemRequest,
                 passed: bool,
                 endpoint: str,
                 latency: float,
                 attempts: int,
                 can_retry: bool = True,
                 thread: Optional[str] = None,
                 cached: Optional[bool] = False,
                 error_type: Optional[str] = None,
                 failed_reason: Optional[str] = None,
                 ):
        self.filepath: str = request.filepath
        self.passed: bool = passed
        self.endpoint: str = endpoint
        self.latency: float = latency
        self.attempts: int = attempts
        self.can_retry: bool = can_retry
        self.thread: Optional[str] = thread
        self.cached: Optional[bool] = cached
        self.error_type: Optional[str] = error_type
        self.failed_reason: Optional[str] = failed_reason

    def serialize_json(self):
        return jsonpickle.encode(self)

    def to_dict(self):
        return copy.deepcopy(self.__dict__)
