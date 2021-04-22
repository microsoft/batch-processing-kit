# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import multiprocessing
from multiprocessing import current_process, RLock
import os
import traceback
from abc import ABC, abstractmethod
from typing import Optional, List
import cProfile

from batchkit.logger import LogEventQueue
from batchkit.utils import BadRequestError
from batchkit.work_item import WorkItemRequest, WorkItemResult


# Process-wide (work item pool worker) globals for CancellationToken, LogEventQueue, WorkItemLock_GlobalScope.
# These are objects that may not be serializable and are needed by all work items that a pool worker proc processes.
proc_scope_ct: Optional[multiprocessing.Event] = None  # Often different between pool worker generations.
proc_scope_leq: Optional[LogEventQueue] = None
proc_scope_global_workitem_lock: Optional[RLock] = None


def init_proc_scope(
        cancellation_token: multiprocessing.Event, log_event_queue: LogEventQueue,
        global_workitem_lock: RLock):
    global proc_scope_ct, proc_scope_leq, proc_scope_global_workitem_lock
    proc_scope_ct = cancellation_token
    proc_scope_leq = log_event_queue
    proc_scope_global_workitem_lock = global_workitem_lock


class WorkItemProcessor(ABC):
    def __init__(self):
        pass

    def process(self,
                work_item: WorkItemRequest,
                endpoint_config: dict, rtf: float,
                enable_profiling: bool = False,

                # Work-item specifics do not need to be provided if they can be defaulted
                # from process scope via init_proc_scope().
                log_event_queue: Optional[LogEventQueue] = None,
                cancellation_token: Optional[multiprocessing.Event] = None,
                global_workitem_lock: Optional[RLock] = None):
        """
        Process a work item.
        """

        leq: LogEventQueue = log_event_queue if log_event_queue else proc_scope_leq
        ct: multiprocessing.Event = cancellation_token if cancellation_token else proc_scope_ct
        gwil: RLock = global_workitem_lock if global_workitem_lock else proc_scope_global_workitem_lock

        # Check that this WorkItemProcessor is capable of procesing the `work_item` type.
        if type(work_item) not in self.work_item_types():
            msg = "{0}: Called to process a work item of unsupported type: {1}. " + \
                  "This WorkItemProcessor is specified as handling work items of these types: {2}.".format(
                    type(self).__name__, type(work_item).__name__, self.work_item_types())
            leq.critical(msg)
            raise BadRequestError(msg)

        if enable_profiling:
            pr = cProfile.Profile()
            pr.enable()

        leq.info("Process: {0} starting to process work item of Type: {1}  and Filepath: {2}".format(
            current_process().name, type(work_item).__name__, work_item.filepath))
        try:
            result: WorkItemResult = self.process_impl(
                work_item, endpoint_config, rtf, leq, ct, gwil)
        except Exception as err:
            tb = traceback.format_exc()
            leq.warning("{0}: Exception in WorkItemProcessor.process(): {1}\n{2}".format(
                type(self).__name__, type(err).__name__, tb))
            raise err
        leq.info("Process: {0} finished processing work item of Type: {1}  and Filepath: {2}".format(
            current_process().name, type(work_item).__name__, work_item.filepath))

        if enable_profiling:
            pr.disable()
            pr.dump_stats("/tmp/{0}_profile".format(os.path.basename(work_item.filepath)))

        return result

    @abstractmethod
    def work_item_types(self) -> List[type]:
        """
        This is a framework template method for which subtypes must provide implementation.
        It should return a List[type] specifying the subtypes of WorkItemRequest that this
        WorkItemProcessor is capable of processing. Each of those WorkItemRequest subtypes
        must be processable by the `process_impl() override provided.
        """
        pass

    @abstractmethod
    def process_impl(self,
                     work_item: WorkItemRequest,
                     endpoint_config: dict, rtf: float,
                     log_event_queue: LogEventQueue, cancellation_token: multiprocessing.Event,
                     global_workitem_lock: RLock) -> WorkItemResult:
        """
        Application-specific implementation of how to process a work item.
        This is a framework template method for which subtypes must provide implementation.
        The implementation must be able to handle any subtype of WorkItemRequest that
        is listed in the `work_item_types()` method which one must also override.
        """
        pass


class StubWorkItemProcessor(WorkItemProcessor):
    """
    Instantiable WorkItemProcessor that is not capable of processing any WorkItemRequest type.
    This is for testing or null instance equivalent. It will fail process() because it supports
    an empty list of WorkItemRequest subtypes.
    """
    def __init__(self):
        super().__init__()

    def process_impl(self, work_item: WorkItemRequest, endpoint_config: dict, rtf: float,
                     log_event_queue: LogEventQueue, cancellation_token: multiprocessing.Event,
                     global_workitem_lock: RLock) -> WorkItemResult:
        raise BadRequestError("StubWorkItemProcessor is not intended for application.")

    def work_item_types(self) -> List[type]:
        return []
