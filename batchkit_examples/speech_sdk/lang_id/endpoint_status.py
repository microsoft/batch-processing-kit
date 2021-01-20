# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import socket
import ipaddress

from batchkit.endpoint_status import EndpointStatusChecker
from batchkit.logger import LogEventQueue


def is_host_name(host):
    try:
        ipaddress.ip_address(host)
        return False
    except ValueError:
        return True


class LangIdEndpointStatusChecker(EndpointStatusChecker):
    def __init__(self, log_event_queue: LogEventQueue):
        super().__init__(log_event_queue)

    def check_endpoint(self, host: str, port: int, is_secure: bool, is_cloud_service: bool):
        if is_host_name(host):
            try:
                host = socket.gethostbyname(host)
            except socket.gaierror as e:
                self.log_event_queue.warning(
                    "{0}: Unable to resolve hostname: '{1}' Error details: {2}".format(
                        type(self), host, e.__repr__()))
                return False

        # Now `host` is an ipaddr.
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(3.0)
            try:
                s.connect((host, port))
            except socket.timeout as e:
                self.log_event_queue.warning(
                    "{0}: Timed out trying to open connection (trying to get SYN-ACK) "
                    "to host: {1} on port: {2} Error details: {3}".format(
                        type(self), host, port, e.__repr__()))
                return False
        return True
