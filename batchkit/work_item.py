# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import copy
from abc import ABC, abstractmethod
from typing import Optional, List, Tuple
import jsonpickle
import heapq

from batchkit.logger import LogEventQueue


class WorkItemRequest(ABC):
    def __init__(self, filepath: str, language: Optional[str] = None):
        self.filepath: str = filepath
        self.language: Optional[str] = language

    def serialize_json(self):
        return jsonpickle.encode(self)

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


class WorkItemQueue:
    """
    Non-thread-safe priority queue for work items.
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
