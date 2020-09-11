# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import multiprocessing
from typing import List, Optional

from batchkit.logger import LogEventQueue
from batchkit.work_item import WorkItemRequest, WorkItemResult
from batchkit_examples.speech_sdk.work_item import SpeechSDKWorkItemResult


class LangIdWorkItemRequest(WorkItemRequest):
    def __init__(self, filepath: str, candidate_languages: List[str],
                 cache_search_dirs: List[str], output_dir: str, log_dir: str):
        """
        :param filepath: input audio file to recognize
        :param candidate_languages: superset of possible languages to consider
        :param output_dir: where wavptr's containing the file's language segments
                           will be placed.
        :param cache_search_dirs: directories where the language segment wavptr's may be
                                  located if the file has been processed before.
        :param log_dir: where per-worker-request SpeechSDK logs will be placed for LID requests
        """
        super().__init__(filepath, None)
        self.candidate_languages: List[str] = candidate_languages
        self.cache_search_dirs: List[str] = cache_search_dirs
        self.output_dir: str = output_dir
        self.log_dir: str = log_dir

    def process_impl(self, endpoint_config: dict, rtf: float,
                     log_event_queue: LogEventQueue, cancellation_token: multiprocessing.Event):
        from .classify import run_classifier
        return run_classifier(
            self,
            rtf,
            endpoint_config,
            log_event_queue,
            cancellation_token
        )


class LangIdWorkItemResult(SpeechSDKWorkItemResult):
    def __init__(self,
                 request: WorkItemRequest,
                 passed: bool,
                 endpoint: str,
                 latency: float,
                 attempts: int,
                 thread: str,
                 can_retry: bool,
                 cached: bool,
                 audio_duration: int,
                 error_type: Optional[str] = None,
                 failed_reason: Optional[str] = None,
                 ):
        super().__init__(
            request,
            passed,
            endpoint,
            latency,
            attempts,
            thread,
            can_retry,
            cached,
            audio_duration,
            error_type,
            failed_reason
        )
