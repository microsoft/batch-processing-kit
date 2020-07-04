# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import inspect
import pkgutil
import warnings
from abc import ABC, abstractmethod
from importlib import import_module
from typing import List
import os
from uuid import uuid4
import jsonpickle
import logging

from .batch_config import BatchConfig
from .endpoint_status import EndpointStatusChecker, UnknownEndpointStatusChecker
from .logger import LogEventQueue
from .run_summarizer import BatchRunSummarizer
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
    def find_subtypes() -> List[type]:
        """
        Find all non-framework (developer implemented) subtypes of BatchRequest.
        Candidates must following this contract:
         - Must be in the type hierarchy originating from abstract BatchRequest.
         - Must be in a module named batch_request.py
         - Must be discoverable from the sys.path (e.g. in an installed package,
           in the PYTHONPATH, etc).
        :returns: A List[type] of BatchRequest subtypes found.
        """
        subtypes: List[type] = []
        with warnings.catch_warnings(record=False):
            warnings.simplefilter("ignore")
            # Also known to be noisy:
            logging.getLogger("pip").setLevel(logging.WARNING)

            for modinfo in pkgutil.walk_packages():
                if not modinfo.ispkg and "batch_request" in modinfo.name:
                    module = import_module(modinfo.name)
                    for elem_name in dir(module):
                        elem = getattr(module, elem_name)
                        if inspect.isclass(elem) and issubclass(elem, BatchRequest) and elem != BatchRequest:
                            subtypes.append(elem)
        return subtypes

    @staticmethod
    @abstractmethod
    def from_json(json: dict):
        """
        Factory construct an instance of BatchRequest based on ctor params provided in a dict
        representing deserialized json args (only basic types). It is up to the implementer
        to decide on their own contract for the dict schema.

        To use this, the type must be supplied with key 'type'
        in the json outer level dict, and all fields needed to
        specify that BatchRequest type must be present.

        Each subtype must implement its own implementation of this static method or a
        BadRequestError will be raised.
        """
        if 'type' not in json:
            err = BadRequestError("Missing 'type' key in json dict specifying the batch")
            logger.warning("Unable to parse BatchRequest from json: {0}. Reason: {1}".format(json, str(err)))
            raise err

        cls = None
        candidates: List[type] = BatchRequest.find_subtypes()
        for candid in candidates:
            if json['type'] == candid.__name__:
                cls = candid
                break

        if not cls:
            err = BadRequestError("Request body argument 'type' not found in syspath: {0}".format(cls))
            logger.warning("Unable to parse BatchRequest from json: {0}. Reason: {1}".format(json, str(err)))
            raise err

        # Ensure the developer has included the static `make_from_json()` method
        # and that it takes a single arg. Won't check for dict arg type or return type
        # since subtype definition may not have these annotations. Those would be
        # runtime errors if violated.
        if 'from_json' not in dir(cls) or len(inspect.getfullargspec(cls.from_json).args) != 1 or \
                cls.from_json == BatchRequest.from_json:  # User failed to override the method
            err = BadRequestError(
                "Found definition for {0} but missing user-defined extension for "
                "static method: from_json(json: dict) -> {0}".format(
                    cls.__name__))
            logger.warning("Unable to parse BatchRequest from json: {0}. Reason: {1}".format(json, str(err)))
            raise err

        return cls.from_json(json)

    @staticmethod
    @abstractmethod
    def from_config(files: List[str], config: BatchConfig):
        """
        Factory construct an instance of TBatchRequest based on params provided in a TBatchConfig
        It is up to the implementer to decide on their own contract for how a TBatchConfig
        is used to construct a TBatchRequest.

        Each subtype must implement its own implementation of this static method with the same
        signature (method name, parameter names) or a BadRequestError will be raised. It must not be inherited.

        The type of `config` should be TBatchConfig that is used to make a corresponding TBatchRequest.
        Furthermore, `config` param should be type-annotated with the concrete subtype name.
        Example:

            @staticmethod
            def from_config(files: List[str], config: TBatchConfig)

        """
        cls = None
        candidates: List[type] = BatchRequest.find_subtypes()
        logger.debug("BatchRequest subtypes on sys.path identified: {0}".format(candidates))
        for candid in candidates:
            if 'from_config' in dir(candid) and \
                    candid.from_config != BatchRequest.from_config:  # User actually overrides the static method
                # Inspect the method to ensure it follows expectations.
                argspec = inspect.getfullargspec(candid.from_config)
                if argspec.args == ['files', 'config'] and \
                        argspec.annotations['files'] == List[str] and \
                        argspec.annotations['config'] == type(config):
                    cls = candid

        if not cls:
            err = BadRequestError(
                "Unable to find subtype of BatchRequest in the syspath with signature"
                "like that expected:  "
                "@staticmethod   def from_config(files: List[str], config: {0})".format(type(config).__name__))
            logger.warning("Unable to parse BatchRequest from config of type {0}: {1}. Reason: {2}".format(
                type(config).__name__, config.__dict__, str(err)))
            raise err

        return cls.from_config(files, config)

    @staticmethod
    def find_type(config: BatchConfig) -> type:
        """
        Given a BatchConfig instance, find the corresponding type of BatchRequest.
        :param config: instance of a concrete subtype of BatchConfig.
        :returns: any subtype of BatchRequest that can be found in the sys.path that can be
                  created from that subtype of BatchConfig. See: BatchRequest.from_config() static method.
        """
        return type(BatchRequest.from_config([], config))

    @abstractmethod
    def make_work_items(self, output_dir: str,
                        cache_search_dirs: List[str],
                        log_dir: str) -> List[WorkItemRequest]:
        """
        Make WorkItemRequests based on the BatchRequest.
        This is dependent on the concrete implementation of BatchRequest subtypes, and each subtype
        should have one or more subtypes of WorkItemRequest it can factory.
        """
        pass

    @staticmethod
    def get_endpoint_status_checker(leq: LogEventQueue) -> EndpointStatusChecker:
        """
        Get an EndpointStatusChecker for the kind of endpoints that are capable of
        processing this type of BatchRequest. This should be overridden by the BatchRequest subtype,
        but otherwise a default UnknownEndpointStatusChecker is returned.
        """
        return UnknownEndpointStatusChecker(leq)

    @abstractmethod
    def get_batch_run_summarizer(self) -> BatchRunSummarizer:
        pass

    @staticmethod
    @abstractmethod
    def is_valid_input_file(file: str) -> bool:
        """
        Query whether a particular file appears to be a valid work item.
        :param file: the relative or absolute filepath
        :return: boolean whether the file is a valid work item
        """
        pass
