# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from abc import ABC, abstractmethod
from typing import Dict

from .work_item import WorkItemResult


class BatchRunSummarizer(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def run_summary(self,
                    snap_work_results: Dict[str, WorkItemResult],
                    snap_file_queue_size: int,
                    snap_num_running: int,
                    start_time: float,
                    num_endpoints: int,
                    log_conclusion_msg: bool) -> dict:
        pass
