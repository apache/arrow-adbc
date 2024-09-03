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

ARG VCPKG

FROM debian:12

RUN export DEBIAN_FRONTEND=noninteractive && \
    apt-get update -y && \
    apt-get install -y curl gnupg && \
    echo "deb http://apt.llvm.org/bookworm/ llvm-toolchain-bookworm main" \
         > /etc/apt/sources.list.d/llvm.list && \
    echo "deb-src http://apt.llvm.org/bookworm/ llvm-toolchain-bookworm main" \
         >> /etc/apt/sources.list.d/llvm.list && \
    curl -L https://apt.llvm.org/llvm-snapshot.gpg.key | apt-key add - && \
    apt-get update -y && \
    apt-get install -y clang libc++abi-dev libc++-dev libomp-dev && \
    apt-get clean

RUN export DEBIAN_FRONTEND=noninteractive && \
    apt-get install -y cmake git libpq-dev libsqlite3-dev pkg-config

RUN curl -L -o go.tar.gz https://go.dev/dl/go1.22.5.linux-amd64.tar.gz && \
    tar -C /opt -xvf go.tar.gz && \
    echo 'export PATH=$PATH:/opt/go/bin' | tee -a ~/.bashrc
