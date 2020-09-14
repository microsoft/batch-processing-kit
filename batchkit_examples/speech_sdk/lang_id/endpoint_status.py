# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from batchkit.logger import LogEventQueue
from batchkit.endpoint_status import EndpointStatusChecker
from batchkit_examples.speech_sdk.endpoint_status import SpeechSDKEndpointStatusChecker


class LangIdEndpointStatusChecker(SpeechSDKEndpointStatusChecker):
    """
    Re-use the implementation of the SpeechSDKEndpointStatusChecker since all
    Cognitive Service containers share the same status checking endpoint.
    """

    def __init__(self, log_event_queue: LogEventQueue):
        super().__init__(log_event_queue)
