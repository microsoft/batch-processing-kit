# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
import json
import multiprocessing
from typing import List, Optional
import audiofile
import os

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
        self._cached_duration = None

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
        # Audio file segment ptr file is one kind of work item.
        if self.filepath.endswith(".seg.json"):
            with open(self.filepath, "r") as f:
                meta = json.load(f)
                start_offset_secs = float(meta["start_offset"])
                end_offset_secs = float(meta["end_offset"])
                self._cached_duration = end_offset_secs - start_offset_secs
        # Regular audio file is the common kind of work item.
        else:
            self._cached_duration = audiofile.duration(self.filepath)
        return self._cached_duration


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
