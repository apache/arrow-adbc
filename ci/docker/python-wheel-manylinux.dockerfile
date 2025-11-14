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

ARG MANYLINUX
FROM quay.io/pypa/manylinux${MANYLINUX}:latest

ARG CMAKE=4.1.2
ARG GO
ARG NINJA=1.13.1
ARG PYTHON
ARG VCPKG
ARG TARGETPLATFORM

SHELL ["/bin/bash", "-i", "-c"]
ENTRYPOINT ["/bin/bash", "-i", "-c"]

# -------------------- System Dependencies --------------------
# Some of these dependencies are needed to build things like OpenSSL in vcpkg
RUN ulimit -n 1024 && yum install -y autoconf curl git flex perl-IPC-Cmd unzip wget yum-utils zip
# docker is aliased to podman by AlmaLinux, but we want real Docker
# (podman is just too different)
RUN ulimit -n 1024 && yum remove -y docker
RUN yum-config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo
RUN ulimit -n 1024 && yum install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

# -------------------- Python --------------------
RUN PYTHON_ROOT=$(find /opt/python -name cp${PYTHON/./}-cp${PYTHON/./}) && \
    echo "export PATH=$PYTHON_ROOT/bin:\$PATH" >> /etc/profile.d/python.sh
ENV PATH="/opt/python/cp${PYTHON/./}-cp${PYTHON/./}/bin:${PATH}"

# -------------------- CMake, Go --------------------
RUN mkdir -p /.cache/go-build                         \
    && chmod 777 /.cache/go-build                     \
    && git config --global --add safe.directory /adbc \
    && cp /root/.gitconfig /.gitconfig                \
    && chmod 777 /.gitconfig

RUN if [[ ${TARGETPLATFORM} == "linux/amd64" ]]; then                                                           \
        export ARCH="amd64" CMAKE_ARCH=x86_64;                                                                  \
    elif [[ ${TARGETPLATFORM} == "linux/arm64" ]]; then                                                         \
        export ARCH="arm64" CMAKE_ARCH=aarch64;                                                                 \
    else                                                                                                        \
        echo "Unsupported platform: ${TARGETPLATFORM}";                                                         \
        exit 1;                                                                                                 \
    fi &&                                                                                                       \
    wget --no-verbose -O cmake.tar.gz                                                                           \
      https://github.com/Kitware/CMake/releases/download/v${CMAKE}/cmake-${CMAKE}-linux-${CMAKE_ARCH}.tar.gz && \
    wget --no-verbose -O go.tar.gz https://go.dev/dl/go${GO}.linux-${ARCH}.tar.gz &&                            \
    tar -C /usr/local -xzf cmake.tar.gz &&                                                                      \
    tar -C /usr/local -xzf go.tar.gz &&                                                                         \
    rm -f cmake.tar.gz go.tar.gz

ENV PATH="/usr/local/go/bin:${PATH}"

# -------------------- Ninja --------------------
RUN mkdir -p /tmp/ninja &&                                                                   \
    wget --no-verbose -O - "https://github.com/ninja-build/ninja/archive/v${NINJA}.tar.gz" | \
      tar -xzf - --directory /tmp/ninja --strip-components=1 &&                              \
    pushd /tmp/ninja &&                                                                      \
    ./configure.py --bootstrap &&                                                            \
    mv ninja "/usr/local/bin" &&                                                             \
    rm -rf /tmp/ninja

# -------------------- vcpkg --------------------
ADD ci/scripts/install_vcpkg.sh /
RUN /install_vcpkg.sh /opt/vcpkg ${VCPKG} && rm -f /install_vcpkg.sh
