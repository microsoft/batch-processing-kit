# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from abc import ABC, abstractmethod
import logging

from .logger import LogEventQueue


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


class UnknownEndpointStatusChecker(EndpointStatusChecker):
    def __init__(self, leq: LogEventQueue):
        super().__init__(leq)

    def check_endpoint(self, host: str, port: int, is_secure: bool, is_cloud_service: bool):
        return True
