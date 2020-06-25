# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import socket
import requests
import json

from batchkit.endpoint_status import EndpointStatusChecker
from batchkit.logger import LogEventQueue


class SpeechSDKEndpointStatusChecker(EndpointStatusChecker):

    socket_timeout: int = 10

    def __init__(self, log_event_queue: LogEventQueue):
        super().__init__(log_event_queue)

    def check_endpoint(self, host: str, port: int, is_secure: bool, is_cloud_service: bool):
        """
        Check whether specified server and port are up and running
        :param host: address of the server to check
        :param port: port to check
        :param is_secure: whether the endpoint is secure
        :param is_cloud_service: whether the endpoint is a public cloud endpoint
        :return: whether port on the server is open
        """
        # Create a TCP socket
        try:
            # TODO: Check DNS first since name resolution timeout can be long.
            with socket.create_connection((host, port), timeout=self.socket_timeout):
                # A non-cloud-service endpoint must be an on-prem container endpoint
                # which will have the special /status health check url that all the
                # On-Prem Azure Cognitive Services containers include.
                if not is_cloud_service:
                    # Also expect a healthy response from /status path which
                    # indicates the container's components are overall okay
                    # including api key validity.
                    result = requests.get("http://{0}:{1}/status".format(host, port))
                    result = json.loads(result.text)
                    if result['apiStatus'] == 'Valid' and result['apiStatusMessage'] == 'Api Key is valid.':
                        return True
                    else:
                        self.log_event_queue.warning(
                            "Currently failing to connect to {0} on port {1}".format(host, port))
                        return False
                else:
                    # Opening connection with the Speech Recognition FrontEnd is otherwise assumed sufficient.
                    return True
        except OSError as e:
            self.log_event_queue.warning(
                "Currently failing to connect to {0} on port {1}: {2}".format(host, port, e))
            return False
