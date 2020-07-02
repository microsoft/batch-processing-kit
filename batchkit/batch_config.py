# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from abc import ABC, abstractmethod
from argparse import Namespace
import logging


logger = logging.getLogger("batch")


class BatchConfig(ABC):
    def __init__(self):
        super().__init__()
        self.combine_results: bool = False

    @staticmethod
    @abstractmethod
    def from_args(args: Namespace):
        """
        Create a concrete subtype of BatchConfig initialized
        from command-line provided arguments.

        The framework expects each subtype of BatchConfig to implement this static method
        returning an instance of that subtype from the provided `args`.
        """
        pass

    def __repr__(self):
        return "{0}({1})".format(type(self).__name__,  str(self.__dict__))
