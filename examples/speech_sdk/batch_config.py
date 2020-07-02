# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
from argparse import Namespace

from batchkit.batch_config import BatchConfig


class SpeechSDKBatchConfig(BatchConfig):

    def __init__(self,
                 language: str,
                 nbest: int,
                 diarization: str,
                 profanity: str,
                 sentiment: bool,
                 allow_resume: bool,
                 combine_results: bool):
        super().__init__()
        self.language: str = language
        self.nbest = nbest
        self.diarization = diarization
        self.profanity = profanity
        self.sentiment = sentiment
        self.allow_resume = allow_resume
        self.combine_results = combine_results

    @staticmethod
    def from_args(args: Namespace):
        return SpeechSDKBatchConfig(
            language=args.language,
            nbest=args.nbest,
            diarization=args.diarization_mode,
            profanity=args.profanity_mode,
            sentiment=args.enable_sentiment,
            allow_resume=args.allow_resume,
            combine_results=args.store_combined_json,
        )
