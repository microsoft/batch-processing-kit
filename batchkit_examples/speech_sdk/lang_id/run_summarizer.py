# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import logging
from batchkit_examples.speech_sdk.run_summarizer import SpeechSDKBatchRunSummarizer


logger = logging.getLogger("batch")


class LangIdBatchRunSummarizer(SpeechSDKBatchRunSummarizer):
    """
    Re-use the implementation of the SpeechSDKBatchRunSummarizer because most of the
    fields of the work item are the same and that suffices for the run summary.
    """
    def __init__(self):
        super().__init__()
