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

ARG FROM=ubuntu:jammy
FROM ${FROM}

ENV IMAGE_VERSION=2025-10-31

RUN \
  echo "debconf debconf/frontend select Noninteractive" | \
    debconf-set-selections

RUN \
  echo 'APT::Install-Recommends "false";' > \
    /etc/apt/apt.conf.d/disable-install-recommends

ARG DEBUG
RUN \
  quiet=$([ "${DEBUG}" = "yes" ] || echo "-qq") && \
  apt update ${quiet} && \
  apt install -y -V ${quiet} \
    ca-certificates \
    lsb-release \
    wget && \
  wget https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb && \
  apt install -y -V ${quiet} \
    ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb && \
  rm apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb && \
  grep " $(lsb_release --codename --short) " /etc/apt/sources.list | \
    sed -e "s/$(lsb_release --codename --short)/$(lsb_release --codename --short)-backports/g" > \
    /etc/apt/sources.list.d/backports.list && \
  apt update && \
  apt install -y -V ${quiet} \
    build-essential \
    ccache \
    cmake \
    debhelper \
    devscripts \
    gobject-introspection \
    libarrow-glib-dev \
    libgirepository1.0-dev \
    libpq-dev \
    libsqlite3-dev \
    meson \
    ninja-build \
    pkg-config \
    tzdata \
    valac && \
  apt install -y -V -t $(lsb_release --codename --short)-backports ${quiet} \
    golang-1.21 && \
  apt clean
