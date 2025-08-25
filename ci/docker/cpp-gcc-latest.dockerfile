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

FROM amd64/debian:experimental
ARG GCC
ARG GO

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update -y && \
    apt-get install -y -q cmake curl git gnupg libpq-dev libsqlite3-dev pkg-config && \
    apt-get install -y -q g++-${GCC} gcc-${GCC} && \
    apt-get clean

RUN curl -L -o go.tar.gz https://go.dev/dl/go${GO}.linux-amd64.tar.gz && \
    tar -C /opt -xvf go.tar.gz

ENV PATH=/opt/go/bin:$PATH \
    CC=/usr/bin/gcc-${GCC} \
    CXX=/usr/bin/g++-${GCC}
