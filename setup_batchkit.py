#!/usr/bin/env python3

# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import os
from setuptools import setup, find_packages


# Read description from the README.md
with open("README.md", "r") as fh:
    long_description = fh.read()


# Pull out all dependencies in requirements.txt for the batchkit library only.
rootdir = os.path.dirname(os.path.realpath(__file__))
with open(os.path.join(rootdir, 'requirements.txt')) as reqs:
    required = reqs.read().splitlines()
deps = []
toggle = False
for line in required:
    if "### batchkit" in line:
        toggle = True
        continue
    elif "###" in line:
        toggle = False
    elif len(line) > 0 and line[0] != "#" and toggle:
        deps.append(line)

# Get package version
filename = "version.txt"
with open(filename, "r") as f:
    version = f.read()  

# Package specification for batchkit library.
setup(
    name='batchkit',
    version=version,
    author='Microsoft Azure',
    author_email='andwald@microsoft.com',
    description="Generic batch processing framework for managing the orchestration, dispatch, fault tolerance, and "
                "monitoring of arbitrary work items against many endpoints. Extensible via dependency injection.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url='https://github.com/microsoft/batch-processing-kit',
    packages=["batchkit"],
    install_requires=deps,
    license="MIT",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: POSIX :: Linux",
    ],
    python_requires='>=3.6',
    scripts=[],
)