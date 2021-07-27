# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

# Notes:
#  - Build with working directory set to the repo root.
#  - libgstreamer and plugins need to be installed explicitly here as they are not available as simple pypi packages.

FROM ubuntu:18.04
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update -y && \
    apt-get upgrade -y && \
    apt-get install -y --no-install-recommends \
        python3 python3-pip python3-dev \
        libglib2.0 libgirepository1.0 libasound2 sox \
        libgstreamer1.0-0 gstreamer1.0-plugins-base gstreamer1.0-plugins-good gstreamer1.0-plugins-bad gstreamer1.0-plugins-ugly python3-gst-1.0
ARG BATCHKIT_EXAMPLES_PKG
COPY dist/${BATCHKIT_EXAMPLES_PKG}-* /
RUN python3 -m pip install setuptools wheel && \
    python3 -m pip install ${BATCHKIT_EXAMPLES_PKG}-*.whl

# Temporary in this branch -- just trying a particular build of speech sdk for when posix reads fail which can
# happen on particular network filesystem  drivers e.g. gluster.
COPY batchkit_examples/speech_sdk/azure_cognitiveservices_speech-1.18.0b0.dev22306528-cp36-cp36m-manylinux1_x86_64.whl /
RUN python3 -m pip install azure_cognitiveservices_speech-1.18.0b0.dev22306528-cp36-cp36m-manylinux1_x86_64.whl

ENTRYPOINT ["run-batch-client"]
