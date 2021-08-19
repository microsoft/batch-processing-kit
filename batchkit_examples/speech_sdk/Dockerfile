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

ARG TEST="test"
COPY tests/${TEST}_* /
COPY dist /
COPY dist/batchkit_examples_speechsdk-0.9.9.dev0.tar.gz /
RUN ls -a && \
    exit 1
ARG BATCHKIT_EXAMPLES_PKG="batchkit_examples_speechsdk"
COPY dist/${BATCHKIT_EXAMPLES_PKG}-* /
RUN ls -a && \
    exit 1
RUN ls -a && \
    python3 -m pip install setuptools wheel && \
    python3 -m pip install ${INTERMED_VAR}-*.whl && \
    exit 1
    # python3 -m pip install batchkit_examples_speechsdk-0.9.9.dev0-py3-none-any.whl && \

ENTRYPOINT ["run-batch-client"]