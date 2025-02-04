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

FROM debian:12
ARG GO

RUN export DEBIAN_FRONTEND=noninteractive && \
    apt-get update -y && \
    apt-get install -y curl gnupg jq && \
    # llvmorg-19.1.7 ->
    # 19.1.7 ->
    # 19
    latest_llvm_major_version=$( \
      curl https://api.github.com/repos/llvm/llvm-project/releases/latest | \
        jq -r .tag_name | \
        cut -d- -f2 | \
        cut -d. -f1) && \
    echo "deb http://apt.llvm.org/bookworm/ llvm-toolchain-bookworm-${latest_llvm_major_version} main" \
         > /etc/apt/sources.list.d/llvm.list && \
    echo "deb-src http://apt.llvm.org/bookworm/ llvm-toolchain-bookworm-${latest_llvm_major_version} main" \
         >> /etc/apt/sources.list.d/llvm.list && \
    curl -L https://apt.llvm.org/llvm-snapshot.gpg.key | apt-key add - && \
    apt-get update -y && \
    apt-get install -y clang-${latest_llvm_major_version} libc++abi-${latest_llvm_major_version}-dev libc++-${latest_llvm_major_version}-dev libomp-${latest_llvm_major_version}-dev && \
    ln -s /usr/bin/clang-${latest_llvm_major_version} /usr/bin/clang && \
    ln -s /usr/bin/clang++-${latest_llvm_major_version} /usr/bin/clang++ && \
    apt-get clean

RUN export DEBIAN_FRONTEND=noninteractive && \
    apt-get install -y cmake git libpq-dev libsqlite3-dev pkg-config

RUN curl -L -o go.tar.gz https://go.dev/dl/go${GO}.linux-amd64.tar.gz && \
    tar -C /opt -xvf go.tar.gz

ENV PATH=/opt/go/bin:$PATH \
    CC=/usr/bin/clang \
    CXX=/usr/bin/clang++
