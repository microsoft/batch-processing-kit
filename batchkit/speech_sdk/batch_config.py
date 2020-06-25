# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from batchkit.batch_config import BatchConfig


class SpeechSDKBatchConfig(BatchConfig):
    def __init__(self,
                 language: str,
                 nbest: int,
                 diarization: str,
                 profanity: str,
                 sentiment: bool,
                 allow_resume: bool):
        super().__init__()
        self.language: str = language
        self.nbest = nbest
        self.diarization = diarization
        self.profanity = profanity
        self.sentiment = sentiment
        self.allow_resume = allow_resume
