#!/usr/bin/env python3

# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from setuptools import setup, find_packages

setup(
    name='batchkit',
    version='0.9.0',
    author='Microsoft Azure',
    author_email='andwald@microsoft.com',
    url='https://github.com/microsoft/batch-processing-kit',
    packages=find_packages(),
    license="MIT",
    scripts=['scripts/run-batch-client'],
)
