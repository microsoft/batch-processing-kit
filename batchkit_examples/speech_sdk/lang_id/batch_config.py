# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
from argparse import Namespace
from typing import List

from batchkit.batch_config import BatchConfig


class LangIdBatchConfig(BatchConfig):

    def __init__(self,
                 languages: List[str],
                 max_segment_length: int):
        super().__init__()
        self.languages: List[str] = languages
        self.max_segment_length: int = max_segment_length

    @staticmethod
    def from_args(args: Namespace):
        return LangIdBatchConfig(
            languages=args.language,
            max_segment_length=args.max_segment_length,
        )
