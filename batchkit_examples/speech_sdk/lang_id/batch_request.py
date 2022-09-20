# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from typing import List

from batchkit.batch_request import BatchRequest
from batchkit.logger import LogEventQueue
from batchkit.utils import BadRequestError
from batchkit.work_item import WorkItemRequest
from batchkit.work_item_processor import WorkItemProcessor
from batchkit_examples.speech_sdk.lang_id.batch_config import LangIdBatchConfig
from batchkit_examples.speech_sdk.lang_id.endpoint_status import LangIdEndpointStatusChecker
from batchkit_examples.speech_sdk.lang_id.run_summarizer import LangIdBatchRunSummarizer
from batchkit_examples.speech_sdk.lang_id.work_item import LangIdWorkItemRequest
from batchkit_examples.speech_sdk.lang_id.work_item_processor import LangIdWorkItemProcessor
import batchkit_examples.speech_sdk.audio as audio


class LangIdBatchRequest(BatchRequest):
    def __init__(self,
                 files: List[str],
                 languages: List[str],
                 max_segment_length: int):
        super().__init__(files, False)
        self.languages: List[str] = languages
        self.max_segment_length: int = max_segment_length

    def make_work_items(self, output_dir: str,
                        cache_search_dirs: List[str],
                        log_dir: str) -> List[WorkItemRequest]:
        return [
            LangIdWorkItemRequest(
                f,
                self.languages,
                self.max_segment_length,
                cache_search_dirs,
                output_dir,
                log_dir,
            )
            for f in self.files
        ]

    @staticmethod
    def from_json(json: dict):
        # List[String] args.
        for arg in ['files', 'languages']:
            if arg not in json:
                raise BadRequestError("Missing '{arg}' argument (List[str]) in request body.".format(arg=arg))
            if not isinstance(json[arg], list) or \
                    not all([isinstance(x, str) for x in json[arg]]):
                raise BadRequestError("Request body argument '{arg}' was not List[str]".format(arg=arg))
        # int args.
        for arg in ['max_segment_length']:
            if arg not in json:
                raise BadRequestError("Missing '{arg}' argument (int) in request body.".format(arg=arg))
            if not isinstance(json[arg], int):
                raise BadRequestError("Request body argument '{arg}' was not of type int".format(arg=arg))
        return LangIdBatchRequest(json['files'], json['languages'], json['max_segment_length'])

    @staticmethod
    def from_config(files: List[str], config: LangIdBatchConfig):
        return LangIdBatchRequest(
            files,
            config.languages,
            config.max_segment_length,
        )

    @staticmethod
    def get_endpoint_status_checker(leq: LogEventQueue) -> LangIdEndpointStatusChecker:
        return LangIdEndpointStatusChecker(leq)

    @staticmethod
    def get_work_item_processor() -> WorkItemProcessor:
        return LangIdWorkItemProcessor()

    def get_batch_run_summarizer(self) -> LangIdBatchRunSummarizer:
        return LangIdBatchRunSummarizer()

    @staticmethod
    def is_valid_input_file(file: str) -> bool:
        return audio.is_valid_audio_file(file)
