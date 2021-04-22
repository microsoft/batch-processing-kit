# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import os
from typing import List, Optional
import audiofile

from batchkit.work_item import WorkItemRequest, WorkItemResult
from batchkit_examples.speech_sdk.work_item import SpeechSDKWorkItemResult


class LangIdWorkItemRequest(WorkItemRequest):
    def __init__(self, filepath: str, candidate_languages: List[str], max_segment_length: int,
                 cache_search_dirs: List[str], output_dir: str, log_dir: str):
        """
        :param filepath: input audio file to recognize
        :param candidate_languages: superset of possible languages to consider
        :param max_segment_length: maximum length of language segments imposed by segmentation,
                                   and otherwise longer segments will be cut up.
        :param output_dir: where wavptr's containing the file's language segments
                           will be placed.
        :param cache_search_dirs: directories where the language segment wavptr's may be
                                  located if the file has been processed before.
        :param log_dir: where per-worker-request SpeechSDK logs will be placed for LID requests
        """
        super().__init__(filepath, 'lid')
        self.candidate_languages: List[str] = candidate_languages
        self.max_segment_length: int = max_segment_length
        self.cache_search_dirs: List[str] = cache_search_dirs
        self.output_dir: str = output_dir
        self.log_dir: str = log_dir
        self._cached_duration = None

    # override
    def priority(self) -> int:
        """
        Use the audio's duration as priority, such that longer audio files
        commence processing first to potentially lower overall batch processing time.
        If the duration cannot be fetched from the audio file's header, a default
        priority of -1 is returned signifying the priority could not be determined.
        """
        try:
            return int(self.duration() * 1000)
        except Exception:
            return -1

    def duration(self) -> float:
        """
        Fetch the audio file duration in seconds.
        """
        if self._cached_duration:
            return self._cached_duration
        if not os.path.isfile(self.filepath):
            raise FileNotFoundError("Cannot determine duration because file does not exist.")
        self._cached_duration = audiofile.duration(self.filepath)
        return self._cached_duration


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
