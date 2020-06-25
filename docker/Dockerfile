# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

FROM ubuntu:18.04
RUN apt-get update -y && \
    apt-get upgrade -y && \
    apt-get install -y --no-install-recommends \
        python3 python3-pip python3-dev \
        build-essential libssl1.0.0 libasound2 \
        libgstreamer1.0-0 gstreamer1.0-plugins-base gstreamer1.0-plugins-good gstreamer1.0-plugins-bad gstreamer1.0-plugins-ugly python3-gst-1.0
ARG BATCH_PACKAGE
COPY dist/${BATCH_PACKAGE}-* requirements.txt /
RUN ln -s /usr/bin/pip3 /usr/bin/pip && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    pip install setuptools wheel && \
    pip install -r requirements.txt && \
    pip install ${BATCH_PACKAGE}-*.whl

ENTRYPOINT ["run-batch-client"]