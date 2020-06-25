# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from abc import ABC
import logging

logger = logging.getLogger("batch")


class BatchConfig(ABC):
    def __init__(self):
        super().__init__()
