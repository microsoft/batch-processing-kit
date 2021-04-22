# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import multiprocessing
from typing import List

from batchkit.logger import LogEventQueue
from batchkit.work_item import WorkItemRequest, WorkItemResult
from batchkit.work_item_processor import WorkItemProcessor
from batchkit_examples.speech_sdk.recognize import run_recognizer
from batchkit_examples.speech_sdk.work_item import SpeechSDKWorkItemRequest


class SpeechSDKWorkItemProcessor(WorkItemProcessor):
    def __init__(self):
        super().__init__()

    def work_item_types(self) -> List[type]:
        return [SpeechSDKWorkItemRequest]

    def process_impl(self,
                     work_item: WorkItemRequest,
                     endpoint_config: dict, rtf: float,
                     log_event_queue: LogEventQueue, cancellation_token: multiprocessing.Event,
                     global_workitem_lock: multiprocessing.RLock) -> WorkItemResult:

        assert isinstance(work_item, SpeechSDKWorkItemRequest)
        return run_recognizer(
            work_item,
            rtf,
            endpoint_config,
            log_event_queue,
            cancellation_token
        )
