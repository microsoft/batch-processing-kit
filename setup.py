#!/usr/bin/env python3

# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from setuptools import setup, find_packages

setup(
    name='batch_client',
    version='1.0.0',
    author='Azure Speech Services',
    author_email='andwald@microsoft.com',
    url='https://docs.microsoft.com/en-us/azure/cognitive-services/speech-service/speech-container-batch-processing',
    packages=find_packages(),
    license="MIT",
    scripts=['scripts/run-batch-client'],
)
