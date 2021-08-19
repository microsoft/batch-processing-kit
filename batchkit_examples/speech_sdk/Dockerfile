# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

# Notes:
#  - Build with working directory set to the repo root.
    # docker build . --build-arg BATCHKIT_EXAMPLES_PKG='batchkit_examples_speechsdk' -t speech-test -f batchkit_examples/speech_sdk/Dockerfile
#  - libgstreamer and plugins need to be installed explicitly here as they are not available as simple pypi packages.

FROM ubuntu:18.04
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update -y && \
    apt-get upgrade -y && \
    apt-get install -y --no-install-recommends \
        python3 python3-pip python3-dev \
        libglib2.0 libgirepository1.0 libasound2 sox \
        libgstreamer1.0-0 gstreamer1.0-plugins-base gstreamer1.0-plugins-good gstreamer1.0-plugins-bad gstreamer1.0-plugins-ugly python3-gst-1.0

# value for BATCHKIT_EXAMPLES_PKG should be passed in on build using --build-arg command
# temp comment
ARG BATCHKIT_EXAMPLES_PKG='batchkit_examples_speechsdk'

# batchkit_examples_speechsdk-0.9.9.dev0-py3-none-any.whl
COPY dist / 
RUN ls -a && \
    echo "Batchkit examples pgk environment variable: ${BATCHKIT_EXAMPLES_PKG}" && \
    ls dist -a
COPY dist/${BATCHKIT_EXAMPLES_PKG}-* /
RUN python3 -m pip install setuptools wheel && \
    python3 -m pip install ${BATCHKIT_EXAMPLES_PKG}-*.whl && \
    echo "Building file directly" && \
    python3 -m pip install batchkit_examples_speechsdk-0.9.9.dev0-py3-none-any.whl && \
    exit 1

ENTRYPOINT ["run-batch-client"]