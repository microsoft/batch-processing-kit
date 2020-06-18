# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from abc import ABC, abstractmethod
from typing import List
import os
from uuid import uuid4
import jsonpickle
import logging

from .utils import BadRequestError
from .work_item import WorkItemRequest

logger = logging.getLogger("batch")


class BatchRequest(ABC):

    def __init__(self, files: List[str], combine_results: bool = False):
        self.batch_id = uuid4().int
        self.files = [os.path.abspath(f) for f in files]
        self.combine_results = combine_results

    def serialize_json(self):
        return jsonpickle.encode(self)

    @staticmethod
    def from_json(json: dict):
        """
        Factory construct an instance of BatchRequest from json.
        To use this, the type must be supplied with key 'type'
        in the json outer level dict, and all fields needed to
        specify that BatchRequest type must be present.
        """
        if 'type' not in json:
            err = BadRequestError("Missing 'type' key in json dict specifying the batch")
        else:
            cls = json['type']

            # TODO: This should be done by external dependency injection instead of fixing types here.
            if cls == 'SpeechSDKBatchRequest':
                from .speech_sdk.batch_request import SpeechSDKBatchRequest
                return SpeechSDKBatchRequest.from_json(json)
            elif cls == 'GrpcBatchRequest':
                from .unidec_grpc.batch_request import GrpcBatchRequest
                return GrpcBatchRequest.from_json(json)
            else:
                err = BadRequestError("Request body argument 'type' is unknown: {0}".format(cls))
        logger.warning("Unable to parse BatchRequest from json: {0}. Reason: {1}".format(json, str(err)))
        raise err

    @abstractmethod
    def make_work_items(self, output_dir: str,
                        cache_search_dirs: List[str],
                        log_dir: str) -> List[WorkItemRequest]:
        pass
