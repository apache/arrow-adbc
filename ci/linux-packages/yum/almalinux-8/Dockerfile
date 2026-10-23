# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

ARG FROM=almalinux:8
FROM ${FROM}

ENV IMAGE_VERSION=2026-01-27

ARG DEBUG

RUN \
  quiet=$([ "${DEBUG}" = "yes" ] || echo "--quiet") && \
  yum install -y ${quiet} epel-release && \
  yum install -y https://packages.apache.org/artifactory/arrow/almalinux/$(cut -d: -f5 /etc/system-release-cpe | cut -d. -f1)/apache-arrow-release-latest.rpm && \
  yum install --enablerepo=powertools -y ${quiet} \
    arrow-glib-devel \
    ccache \
    cmake \
    gcc-c++ \
    gobject-introspection-devel \
    libpq-devel \
    make \
    ninja-build \
    pkg-config \
    python3 \
    python3-pip \
    rpmdevtools \
    sqlite-devel \
    tar \
    wget \
    vala && \
  yum clean ${quiet} all

ARG GO_VERSION
RUN ARCH=$(arch) && \
    if [[ ${ARCH} == "x86_64" ]]; then \
        GO_ARCH="amd64"; \
    elif [[ ${ARCH} == "aarch64" ]]; then \
        GO_ARCH="arm64"; \
    else \
        echo "Unsupported platform: ${ARCH}"; \
        exit 1; \
    fi && \
    wget --no-verbose https://go.dev/dl/go${GO_VERSION}.linux-${GO_ARCH}.tar.gz && \
    tar -C /usr/local -xzf go${GO_VERSION}.linux-${GO_ARCH}.tar.gz && \
    rm go${GO_VERSION}.linux-${GO_ARCH}.tar.gz
ENV PATH="/usr/local/go/bin:${PATH}"
