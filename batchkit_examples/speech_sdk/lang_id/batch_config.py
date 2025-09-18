# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from argparse import Namespace
from typing import List

from batchkit.batch_config import BatchConfig


class LangIdBatchConfig(BatchConfig):

    def __init__(self,
                 languages: List[str],
                 max_segment_length: int,
                 lid_timeout: int = 0,
                 recognize_retry: int = 3):
        super().__init__()
        self.languages: List[str] = languages
        self.max_segment_length: int = max_segment_length
        self.lid_timeout: int = lid_timeout  # 0 indicates no timeout.
        self.recognize_retry: int = recognize_retry

    @staticmethod
    def from_args(args: Namespace):
        return LangIdBatchConfig(
            languages=args.language,
            max_segment_length=args.max_segment_length,
            lid_timeout=args.lid_timeout,
            recognize_retry=args.recognize_retry
        )
