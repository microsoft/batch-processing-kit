import logging
from unittest import TestCase, main
from unittest.mock import patch

import batchkit
from batchkit.endpoint_config import normalize_validate_config
from .test_base import MockDevice
from deepdiff import DeepDiff
import json


class ConfigTestCaseBase(TestCase):
    @staticmethod
    def dump_dict(my_dict):
        return "{0}".format(json.dumps(my_dict, indent=2, sort_keys=True))

    def assertEmpty(self, my_dict):
        self.assertEqual(len(my_dict), 0, self.dump_dict(my_dict))

    def assertEmptyNormalized(self, my_dict):
        with patch('sys.stderr', new=MockDevice()):
            validated_dict = normalize_validate_config(my_dict)
        self.assertEmpty(validated_dict)


class ConfigTestCase(ConfigTestCaseBase):
    def test_empty_config(self):
        validated_dict = normalize_validate_config(dict())
        self.assertEmpty(validated_dict)

    def test_default_values(self):
        default_dict = {
            "localhost": {
                "concurrency": 8,
                "host": "localhost",
                "isCloudService": False,
                "isSecure": False,
                "language": "en-US",
                "port": 5000,
                "rtf": 3.0,
                "startConcurrency": 8
            }
        }
        validated_dict = normalize_validate_config({"localhost": dict()})
        self.assertEqual(len(validated_dict), 1)
        deep_diff = DeepDiff(validated_dict, default_dict, ignore_order=True)
        self.assertEmpty(deep_diff)

    def test_invalid_concurrency(self):
        self.assertEmptyNormalized({"localhost": {"concurrency": "suzy"}})
        self.assertEmptyNormalized({"localhost": {"concurrency": 0}})
        self.assertEmptyNormalized({"localhost": {"concurrency": 1001}})

    def test_invalid_language(self):
        self.assertEmptyNormalized({"localhost": {"language": 2}})
        self.assertEmptyNormalized({"localhost": {"language": "sr-SR"}})

    def test_invalid_port(self):
        self.assertEmptyNormalized({"localhost": {"port": "suzy"}})
        self.assertEmptyNormalized({"localhost": {"port": -1}})
        self.assertEmptyNormalized({"localhost": {"port": 100000}})

    def test_invalid_rtf(self):
        self.assertEmptyNormalized({"localhost": {"rtf": "suzy"}})
        self.assertEmptyNormalized({"localhost": {"rtf": 0.0}})
        self.assertEmptyNormalized({"localhost": {"rtf": 10.0}})

    def test_cloud_endpoint(self):
        validated_dict = normalize_validate_config({"localhost": {"host": "westus2.stt.speech.microsoft.com"}})
        self.assertTrue(validated_dict["localhost"]["isCloudService"], self.dump_dict(validated_dict))
        self.assertTrue(validated_dict["localhost"]["isSecure"], self.dump_dict(validated_dict))
        self.assertEqual(validated_dict["localhost"]["port"], 443, self.dump_dict(validated_dict))

    def setUp(self) -> None:
        batchkit.endpoint_config.logger.level = logging.CRITICAL  # Set to DEBUG for debugging.


if __name__ == '__main__':
    main()
