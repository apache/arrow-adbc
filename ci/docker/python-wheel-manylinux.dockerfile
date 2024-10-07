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

ARG ARCH
ARG MANYLINUX
ARG PYTHON
ARG REPO
ARG VCPKG

FROM ${REPO}:${ARCH}-python-${PYTHON}-wheel-manylinux-${MANYLINUX}-vcpkg-${VCPKG}

ARG ARCH
ARG GO

# docker is aliased to podman by AlmaLinux, but we want real Docker
# (podman is just too different)
RUN yum remove -y docker ; yum install -y yum-utils
RUN yum-config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo
RUN yum install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
# arm64v8 -> arm64
RUN wget --no-verbose https://go.dev/dl/go${GO}.linux-${ARCH/v8/}.tar.gz && \
    tar -C /usr/local -xzf go${GO}.linux-${ARCH/v8/}.tar.gz && \
    rm go${GO}.linux-${ARCH/v8/}.tar.gz
ENV PATH="/usr/local/go/bin:${PATH}"
