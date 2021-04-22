# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import multiprocessing
from typing import List

from batchkit.logger import LogEventQueue
from batchkit.work_item import WorkItemRequest, WorkItemResult
from batchkit.work_item_processor import WorkItemProcessor
from batchkit_examples.speech_sdk.lang_id.classify import run_classifier
from batchkit_examples.speech_sdk.lang_id.work_item import LangIdWorkItemRequest


class LangIdWorkItemProcessor(WorkItemProcessor):
    def __init__(self):
        super().__init__()

    def work_item_types(self) -> List[type]:
        return [LangIdWorkItemRequest]

    def process_impl(self,
                     work_item: WorkItemRequest,
                     endpoint_config: dict, rtf: float,
                     log_event_queue: LogEventQueue, cancellation_token: multiprocessing.Event,
                     global_workitem_lock: multiprocessing.RLock) -> WorkItemResult:

        assert isinstance(work_item, LangIdWorkItemRequest)
        return run_classifier(
            work_item,
            rtf,
            endpoint_config,
            log_event_queue,
            cancellation_token
        )
