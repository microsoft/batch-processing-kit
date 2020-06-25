# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import copy
import json
import multiprocessing
import shutil
from collections import defaultdict
from enum import Enum
import os
from typing import List
import jsonpickle
import pyinotify
import logging
import re

from .batch_request import BatchRequest
from .utils import BatchNotFoundException, write_json_file_atomic

logger = logging.getLogger("batch")


class BatchStatusEnum(Enum):
    waiting = 0
    running = 1
    done = 2


class BatchStatus(object):

    def __init__(self, batch_id: int, status: BatchStatusEnum, run_summary_path: str,
                 results_dir: str, combined_results_file: str):
        self.batch_id = batch_id
        self.status = status
        self.run_summary_path = run_summary_path
        self.results_dir = results_dir
        self.combined_results_file = combined_results_file

    def serialize_json(self):
        # We can't json-serialize the actual BatchStatusEnum object.
        cp = copy.deepcopy(self)
        del cp.status
        cp.status = self.status.name
        return jsonpickle.encode(cp)


class BatchStatusProvider(object):
    """
    Uses the local virtual filesystem mounted at a scratch path as a
    persistent database of pending, running, and finished batches, including
    their statuses, evolving run summary, single-file results, and combined
    final results (if done).
    """

    # Can be safely shared across multi-process boundaries and ensure same underlying
    # as long as single module import before forking (or share same instance across forking).
    lock = multiprocessing.RLock()

    def __init__(self, scratch: str):
        self.scratch = scratch

        # The following are to facilitate batch watchers.
        # Lazy initialized for safety with multiproc forking.
        self._batch_watcher_callbacks = None  # {batch_id: [cb1(BatchStatus)->bool, cb2(BatchStatus)->bool, ...]}
        self._wm = None
        self._notifier = None

    def batch_base_path(self, batch_id: int) -> str:
        return os.path.join(self.scratch, 'batch_{batch_id}'.format(batch_id=batch_id))

    def combined_results_path(self, batch_id: int) -> str:
        return os.path.join(self.batch_base_path(batch_id), 'results.json')

    def _status_path(self, batch_id: int) -> str:
        return os.path.join(self.batch_base_path(batch_id), '.status')

    def run_summary_path(self, batch_id: int) -> str:
        return os.path.join(self.batch_base_path(batch_id), 'run_summary.json')

    def assert_batch_exists(self, batch_id: int):
        with BatchStatusProvider.lock:
            if not self.batch_exists(batch_id):
                raise BatchNotFoundException("batch_id: {batch_id} does not exist".format(batch_id=batch_id))

    def batch_exists(self, batch_id: int) -> bool:
        with BatchStatusProvider.lock:
            return os.path.isdir(self.batch_base_path(batch_id))

    def new_batch(self, req: BatchRequest) -> bool:
        with BatchStatusProvider.lock:
            if self.batch_exists(req.batch_id):
                return False
            os.mkdir(self.batch_base_path(req.batch_id))
            self.change_status_enum(req.batch_id, BatchStatusEnum.waiting)
            return True

    def change_status_enum(self, batch_id: int, to_state: BatchStatusEnum):
        with BatchStatusProvider.lock:
            self.assert_batch_exists(batch_id)
            with open(self._status_path(batch_id), 'w') as f:
                f.write(to_state.name)

    def status_enum(self, batch_id: int) -> BatchStatusEnum:
        with BatchStatusProvider.lock:
            self.assert_batch_exists(batch_id)
            with open(self._status_path(batch_id), 'r') as f:
                return BatchStatusEnum[f.readline()]

    def set_run_summary(self, batch_id: int, run_summary: {}):
        """
        Update the run summary for a batch.
        """
        with BatchStatusProvider.lock:
            self.assert_batch_exists(batch_id)
        # Assume batch dir not deleted anytime soon, and write atomically, so no lock.
        write_json_file_atomic(run_summary, self.run_summary_path(batch_id))

    def run_summary(self, batch_id: int):
        """
        Get the latest run summary available for a batch.
        """
        with BatchStatusProvider.lock:
            self.assert_batch_exists(batch_id)
        # Can read without lock since writing is done atomically.
        try:
            with open(self.run_summary_path(batch_id), 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            return {}

    @staticmethod
    def associated_batch_id(path: str) -> int:
        """
        Given a path to any status or output file associated with a batch,
        determine the id of the batch it belongs to.
        :returns: a batch id or -1 if no batch associated with file.
        """
        match_obj = re.match(r'.*batch_(\d+)/?.*', path, flags=0)
        if match_obj:
            return int(match_obj.group(1))
        return -1

    def status(self, batch_id: int) -> BatchStatus:
        with BatchStatusProvider.lock:
            self.assert_batch_exists(batch_id)
            status_enum = self.status_enum(batch_id)
            run_summary_path = self.run_summary_path(batch_id)
            results_dir = self.batch_base_path(batch_id) \
                if status_enum != BatchStatusEnum.waiting \
                else None
            combined_results_file = self.combined_results_path(batch_id) \
                if status_enum == BatchStatusEnum.done \
                else None
            return BatchStatus(
                batch_id, status_enum, run_summary_path, results_dir, combined_results_file)

    def batch_dirs(self) -> List[str]:
        """
        Return a list of all the batch dirs.
        """
        return [os.path.join(self.scratch, d) for d in os.listdir(self.scratch)
                if os.path.isdir(os.path.join(self.scratch, d))
                and self.associated_batch_id(d + "/.") != -1]

    def rm_batch(self, batch_id: int):
        """
        Delete a batch's path in the scratch space, including any results
        and run summary information. The batch will appear not to exist hereafter.
        """
        logger.info("BatchStatusProvider: rm_batch(): Removing batch_id: {0}".format(batch_id))

        path = self.batch_base_path(batch_id)
        shutil.rmtree(path, ignore_errors=True)

    def register_watch(self, batch_id: int, target_state: BatchStatusEnum) -> multiprocessing.Event:
        """
        Places a particular batch under watch for status changes and
        returns an Event object that will be toggled when the target
        status change occurs. If the status is already at target,
        the Event will be toggled immediately. It is also possible
        that the batch is beyond the target state or the transition
        beyond the target state happens so rapidly that the Event
        may be toggled "late".
        """
        evt = multiprocessing.Event()
        with BatchStatusProvider.lock:
            if self.status_enum(batch_id).value >= target_state.value:
                evt.set()
                return evt

            # Lazy init watch manager and thread-based notifier.
            self._init_inotify()

            # Will be invoked when a batch's .status is modified.
            # Returns whether the callback should still be called again.
            def callback(status: BatchStatus):
                if status.status.value >= target_state.value:
                    evt.set()
                    return False
                return True

            # Add this batch's .status path under watch and
            # add register the callback for when it's modified.
            self._batch_watcher_callbacks[batch_id].append(callback)
            self._wm.add_watch(self._status_path(batch_id), pyinotify.IN_CLOSE_WRITE)

            # We would be vulnerable to a race condition were it not for
            # this one more check.
            if self.status_enum(batch_id).value >= target_state.value:
                evt.set()
                # Leave the inotify watch. No need to cancel it because the
                # event is toggled idempotently.
        return evt

    class _BatchStatusChangeHandler(pyinotify.ProcessEvent):
        def my_init(self, parent):
            self.parent = parent

        def process_IN_CLOSE_WRITE(self, event):
            # Which batch being watched was touched?
            batch_id = BatchStatusProvider.associated_batch_id(event.path)
            with BatchStatusProvider.lock:
                # Is anyone actually watching this batch anymore?
                cbs = self.parent._batch_watcher_callbacks[batch_id]
                if len(cbs) > 0:
                    status = self.parent.status(batch_id)
                    # Invoke the callbacks and each one tells us whether it
                    # can be removed or needs to remain.
                    keep_cbs = []
                    for cb in cbs:
                        if cb(status):
                            keep_cbs.append(cb)
                    self.parent._batch_watcher_callbacks[batch_id] = keep_cbs
                    cbs = keep_cbs

                # Now decide if we can eliminate inotify on this path.
                if len(cbs) == 0:
                    # Nobody watching this batch anymore, cancel it.
                    wd = self.parent._wm.get_wd(event.path)
                    if wd:
                        self.parent._wm.rm_watch(wd)


    def _init_inotify(self):
        with BatchStatusProvider.lock:
            if self._wm is not None:
                return
            self._batch_watcher_callbacks = defaultdict(list)
            self._wm = pyinotify.WatchManager()
            self._notifier = pyinotify.ThreadedNotifier(
                self._wm,
                self._BatchStatusChangeHandler(parent=self)
            )
            self._notifier.daemon = True
            self._notifier.name = "BatchStatusChangeHandlerThread"
            self._notifier.start()


