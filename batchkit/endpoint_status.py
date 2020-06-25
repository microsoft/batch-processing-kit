# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from abc import ABC, abstractmethod
import logging

from .logger import LogEventQueue
from .utils import BadRequestError

logger = logging.getLogger("batch")


class EndpointStatusChecker(ABC):
    def __init__(self, log_event_queue: LogEventQueue):
        super().__init__()
        self.log_event_queue = log_event_queue

    @abstractmethod
    def check_endpoint(self, host: str, port: int, is_secure: bool, is_cloud_service: bool):
        """
        Check whether specified server and port are up and running
        :param host: address of the server to check
        :param port: port to check
        :param is_secure: whether the endpoint is secure
        :param is_cloud_service: whether the endpoint is a public cloud endpoint
        :return: whether port on the server is open
        """
        pass

    @staticmethod
    def get_for_type(req_type: type, leq: LogEventQueue):
        """
        Factory construct an instance of EndpointStatusChecker based on
        the type of a BatchRequest.
        :param req_type: the type of BatchRequest, which should be a concrete subtype.
        :param leq: an instance of LogEventQueue for logging in multi-proc setting.
        """
        # TODO: This should be done by external dependency injection instead of fixing types here.
        if req_type.__name__ == "SpeechSDKBatchRequest":
            from batchkit.speech_sdk.endpoint_status import SpeechSDKEndpointStatusChecker
            return SpeechSDKEndpointStatusChecker(leq)
        elif req_type.__name__ == "GrpcBatchRequest":
            from batchkit.unidec_grpc.endpoint_status import GrpcEndpointStatusChecker
            return GrpcEndpointStatusChecker(leq)
        elif isinstance(None, req_type):
            return UnknownEndpointStatusChecker(leq)
        else:
            err = BadRequestError("Unknown BatchRequest type {0}".format(req_type))
        logger.warning("Unable to provide an EndpointStatusChecker: {0}".format(str(err)))
        raise err


class UnknownEndpointStatusChecker(EndpointStatusChecker):
    def __init__(self, leq: LogEventQueue):
        super().__init__(leq)

    def check_endpoint(self, host: str, port: int, is_secure: bool, is_cloud_service: bool):
        return True
