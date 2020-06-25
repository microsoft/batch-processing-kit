# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import logging
import json
import yaml
import os
from cerberus import Validator
from .utils import InvalidConfigurationError


logger = logging.getLogger("batch")


# @TODO: Serialize this into a file, and use attrdict or named tuple to access fields
endpoint_schema = {
    "host": {
        "type": "string",
        "default_setter": lambda doc: get_localhost_name()
    },
    "isCloudService": {
        "type": "boolean",
        "default_setter": lambda doc: ".stt.speech.microsoft.com" in doc["host"]
    },
    "isSecure": {
        "type": "boolean",
        "default_setter": lambda doc: doc["isCloudService"]
    },
    "concurrency": {
        "type": "integer",
        "default": 8,
        "min": 1,
        "max": 1000
    },
    "port": {
        "type": "integer",
        "default_setter": lambda doc: 443 if doc["isSecure"] else 5000,
        "min": 0,
        "max": 65535
    },
    "rtf": {
        "type": "float",
        "default": 3.0,
        "min": 0.1,
        "max": 5.0
    },
    "subscription": {
        "type": "string",
        "nullable": False,
        "coerce": lambda x: str(os.environ.get(x[1:])) if x[0] == "$" else x
    },
    "language": {
        "type": "string",
        "default": "en-US",
        "allowed": ["ar-BH",
                    "ar-EG",
                    "ar-SA",
                    "ar-SY",
                    "ca-ES",
                    "da-DK",
                    "de-DE",
                    "en-US",
                    "en-AU",
                    "en-CA",
                    "en-GB",
                    "en-IN",
                    "en-NZ",
                    "es-ES",
                    "es-MX",
                    "fi-FI",
                    "fr-FR",
                    "fr-CA",
                    "hi-IN",
                    "it-IT",
                    "ja-JP",
                    "ko-KR",
                    "nb-NO",
                    "nl-NL",
                    "pl-PL",
                    "pt-PT",
                    "pt-BR",
                    "ru-RU",
                    "sv-SE",
                    "th-TH",
                    "zh-HK",
                    "zh-CN",
                    "zh-TW"]

    },
    "startConcurrency": {
        "type": "integer",
        "default_setter": lambda doc: doc["concurrency"],
        "min": 1,
        "max": 1000
    }
}


def load_configuration(config_file, strict_validation=False):
    """
    Load the configuration settings if needed
    :param config_file: configuration file
    :param strict_validation: whether to fail in the case of bad configuration entry
    :return: dictionary with configuration data
    """
    config_data = dict()
    error_str = None
    if not os.path.isfile(config_file):
        error_str = "Configuration file {0} does not exist".format(config_file)
        logger.error(error_str)
        logger.info("Defaulting to localhost basic configuration for local testing")
        config_data["localhost"] = dict()
    elif os.path.splitext(config_file)[1].lower() == ".json":
        try:
            with open(config_file) as cf:
                config_data = json.load(cf)
        except Exception as e:
            error_str = "Could not load configuration file {0}: {1}".format(config_file, e)
            logger.error(error_str)
    elif os.path.splitext(config_file)[1].lower() in [".yaml", ".yml"]:
        try:
            with open(config_file) as cf:
                config_data = yaml.safe_load(cf)
        except Exception as e:
            error_str = "Could not load configuration file {0}: {1}".format(config_file, e)
            logger.error(error_str)
    else:
        error_str = "Unsupported configuration file extension. Supported: *.yaml or *.json"
        logger.error(error_str)

    if strict_validation and error_str is not None:
        raise InvalidConfigurationError(error_str)

    return normalize_validate_config(config_data, strict_validation)


def normalize_validate_config(config_data, strict_validation=False):
    """
    Normalize and validate endpoints
    :param config_data: dictionary with endpoints
    :param strict_validation: whether to fail in the case of bad configuration entry
    :return: dictionary with normalized and validated endpoints
    """
    validated_config_data = dict()
    v = Validator(endpoint_schema)
    error_str = None
    error_list = list()

    for endpoint_key, endpoint_value in config_data.items():
        normalized_item = v.normalized(endpoint_value)
        validated_endpoint = v.validated(normalized_item)
        if validated_endpoint is None:
            error_str = "Schema failed to validate for endpoint {0}, removing it. Errors: {1}".format(
                endpoint_key, json.dumps(v.errors, indent=2, sort_keys=True))
            error_list.append(error_str)
            logger.error(error_str)
        else:
            validated_config_data[endpoint_key] = validated_endpoint

    logger.debug("Original configuration:")
    for k, v in config_data.items():
        logger.debug("{0}: {1}".format(k, json.dumps(v, indent=2, sort_keys=True)))
    logger.debug("Normalized configuration:")
    for k, v in validated_config_data.items():
        logger.debug("{0}: {1}".format(k, json.dumps(v, indent=2, sort_keys=True)))

    if strict_validation and len(error_list) > 0:
        raise InvalidConfigurationError("\n".join(error_list))

    return validated_config_data


def get_localhost_name():
    """
    Get a name to use to talk to the host machine
    :return: the name based on Docker implementation (Windows/Mac vs. Linux)
    """
    is_mac_or_win = False
    with open('/etc/resolv.conf') as f:
        for line in f.readlines():
            if 'nameserver 192.168.' in line:
                is_mac_or_win = True

    return "host.docker.internal" if is_mac_or_win else "localhost"


