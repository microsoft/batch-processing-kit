# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import multiprocessing
from typing import List, Optional

from batchkit.logger import LogEventQueue
from batchkit.work_item import WorkItemRequest, WorkItemResult


class SpeechSDKWorkItemRequest(WorkItemRequest):
    def __init__(self, filepath: str, language: str,
                 nbest: int, diarization: str, profanity: str,
                 cache_search_dirs: List[str], output_dir: str,
                 log_dir: str, allow_resume: bool, enable_sentiment: bool):
        """
        :param filepath: input audio file to recognize
        :param language: language of the request
        :param nbest: how many maximum results to consider per recognition
        :param diarization: diarization mode
        :param profanity: profanity mode
        :param output_dir: where json result containing the file's transcriptions
                              details will be placed.
        :param cache_search_dirs: directories where the audio file's json result may
                                  be located if it has already been processed before.
        :param log_dir: where per-worker-request Carbon SDK logs will be placed
        """
        super().__init__(filepath, language)
        self.nbest = nbest
        self.diarization = diarization
        self.profanity = profanity
        self.cache_search_dirs = cache_search_dirs
        self.output_folder = output_dir
        self.log_dir = log_dir
        self.allow_resume = allow_resume
        self.enable_sentiment = enable_sentiment

    def process_impl(self, endpoint_config: dict, rtf: float,
                     log_event_queue: LogEventQueue, cancellation_token: multiprocessing.Event):
        from .recognize import run_recognizer
        return run_recognizer(
            self,
            rtf,
            endpoint_config,
            log_event_queue,
            cancellation_token
        )


class SpeechSDKWorkItemResult(WorkItemResult):
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
        super().__init__(request, passed, endpoint, latency, attempts,
                         can_retry, thread, cached, error_type, failed_reason)
        self.audio_duration = audio_duration
