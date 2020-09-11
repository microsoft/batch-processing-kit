# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
from argparse import Namespace
from typing import List

from batchkit.batch_config import BatchConfig


class LangIdBatchConfig(BatchConfig):

    def __init__(self,
                 languages: List[str]):
        super().__init__()
        self.languages: List[str] = languages

    @staticmethod
    def from_args(args: Namespace):
        return LangIdBatchConfig(
            languages=args.language,
        )
