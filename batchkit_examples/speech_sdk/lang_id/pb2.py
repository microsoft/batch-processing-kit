# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import os
import sys


def get_script_dir():
    return os.path.dirname(os.path.realpath(__file__))


def get_script_path():
    return os.path.realpath(__file__)


protogen_path = os.path.join(get_script_dir(), 'proto')

sys.path.insert(0, protogen_path)

from LIDRequestMessage_pb2 import LIDRequestMessage
from AudioConfig_pb2 import AudioConfig
from LanguageIdRpc_pb2_grpc import LanguageIdStub
import IdentifierConfig_pb2
from IdentifierConfig_pb2 import IdentifierConfig
from IdentificationCompletedMessage_pb2 import IdentificationCompletedMessage
from FinalResultMessage_pb2 import FinalResultMessage