# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

# Notes:
#  - Build with working directory set to the repo root.
#  - libgstreamer and plugins need to be installed explicitly here as they are not available as simple pypi packages.

FROM ubuntu:20.04
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update -y && \
    apt-get upgrade -y && \
    apt-get install -y --no-install-recommends \
        python3 python3-pip python3-dev \
        libgirepository1.0 libasound2 sox \
        libgstreamer1.0-0 gstreamer1.0-plugins-base gstreamer1.0-plugins-good gstreamer1.0-plugins-bad gstreamer1.0-plugins-ugly python3-gst-1.0
ARG BATCHKIT_EXAMPLES_PKG
COPY dist/${BATCHKIT_EXAMPLES_PKG}-* /
RUN python3 -m pip install setuptools wheel && \
    python3 -m pip install ${BATCHKIT_EXAMPLES_PKG}-*.whl

ENTRYPOINT ["run-batch-client"]
