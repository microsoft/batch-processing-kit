# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from abc import ABC, abstractmethod
from typing import Dict

from .batch_request import BatchRequest
from .work_item import WorkItemResult


class BatchRunSummarizer(ABC):
    def __init__(self):
        pass

    @staticmethod
    def create(req: BatchRequest):

        # TODO: This should be done by external dependency injection instead of fixing types here.
        cls = type(req)
        if cls.__name__ == 'SpeechSDKBatchRequest':
            from .speech_sdk.run_summarizer import SpeechSDKBatchRunSummarizer
            return SpeechSDKBatchRunSummarizer()
        elif cls.__name__ == 'GrpcBatchRequest':
            from .unidec_grpc.run_summarizer import GrpcBatchRunSummarizer
            return GrpcBatchRunSummarizer()
        else:
            raise TypeError("BatchRunSummarizer:  BatchRequest of type {0} unknown".format(cls))

    @abstractmethod
    def run_summary(self,
                    snap_work_results: Dict[str, WorkItemResult],
                    snap_file_queue_size: int,
                    snap_num_running: int,
                    start_time: float,
                    num_endpoints: int,
                    log_conclusion_msg: bool) -> dict:
        pass


