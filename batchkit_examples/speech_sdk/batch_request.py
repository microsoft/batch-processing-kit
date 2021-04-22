# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from typing import List

from batchkit.batch_request import BatchRequest
from batchkit.logger import LogEventQueue
from batchkit.utils import BadRequestError
from batchkit.work_item import WorkItemRequest
from batchkit.work_item_processor import WorkItemProcessor

from batchkit_examples.speech_sdk.batch_config import SpeechSDKBatchConfig
from batchkit_examples.speech_sdk.endpoint_status import SpeechSDKEndpointStatusChecker
from batchkit_examples.speech_sdk.run_summarizer import SpeechSDKBatchRunSummarizer
from batchkit_examples.speech_sdk.work_item import SpeechSDKWorkItemRequest
import batchkit_examples.speech_sdk.audio as audio
from batchkit_examples.speech_sdk.work_item_processor import SpeechSDKWorkItemProcessor


class SpeechSDKBatchRequest(BatchRequest):

    def __init__(self, files: List[str],
                 language: str, diarization: str, nbest: int, profanity: str,
                 allow_resume: bool, enable_sentiment: bool, combine_results: bool = False):
        super().__init__(files, combine_results)
        # TODO: We need to support all the options available on the
        #       Azure Cognitive Services on-cloud batch service.
        self.language = language
        self.diarization = diarization
        self.nbest = nbest
        self.profanity = profanity
        self.allow_resume = allow_resume
        self.enable_sentiment = enable_sentiment

    def make_work_items(self, output_dir: str,
                        cache_search_dirs: List[str],
                        log_dir: str) -> List[WorkItemRequest]:
        return [
            SpeechSDKWorkItemRequest(
                f,
                self.language,
                self.nbest,
                self.diarization,
                self.profanity,
                cache_search_dirs,
                output_dir,
                log_dir,
                self.allow_resume,
                self.enable_sentiment,
            )
            for f in self.files
        ]

    @staticmethod
    def from_json(json: dict):
        # Ensure all the args are present.
        for arg in ['files', 'language', 'diarization', 'nbest',
                    'profanity', 'allow_resume', 'sentiment',
                    'combine_results']:
            if arg not in json:
                raise BadRequestError("Missing '{arg}' argument (string) in request body".format(arg=arg))
        # Integer args.
        for arg in ['nbest']:
            if not isinstance(json[arg], int):
                raise BadRequestError("Request body argument '{0}' was not an integer: '{1}'".format(arg, json[arg]))
        # Boolean args.
        for arg in ['allow_resume', 'sentiment', 'combine_results']:
            if str.lower(str(json[arg])) not in ["true", "false"]:
                raise BadRequestError("'{arg}' argument needs boolean in request body".format(arg=arg))
            json[arg] = BatchRequest.to_bool(json[arg])
        # File List arg.
        if not isinstance(json['files'], list) or \
                not all([isinstance(x, str) for x in json['files']]):
            raise BadRequestError("Request body argument 'files' was not List[str]")
        return SpeechSDKBatchRequest(json['files'], json['language'], json['diarization'],
                                     json['nbest'], json['profanity'], json['allow_resume'],
                                     json['sentiment'], json['combine_results'])

    @staticmethod
    def from_config(files: List[str], config: SpeechSDKBatchConfig):
        return SpeechSDKBatchRequest(
            files,
            config.language,
            config.diarization,
            config.nbest,
            config.profanity,
            config.allow_resume,
            config.sentiment,
            config.combine_results,
        )

    @staticmethod
    def get_endpoint_status_checker(leq: LogEventQueue) -> SpeechSDKEndpointStatusChecker:
        return SpeechSDKEndpointStatusChecker(leq)

    @staticmethod
    def get_work_item_processor() -> WorkItemProcessor:
        return SpeechSDKWorkItemProcessor()

    def get_batch_run_summarizer(self) -> SpeechSDKBatchRunSummarizer:
        return SpeechSDKBatchRunSummarizer()

    @staticmethod
    def is_valid_input_file(file: str) -> bool:
        return audio.is_valid_audio_file(file)
