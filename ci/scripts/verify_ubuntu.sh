#!/bin/bash
#
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
#

# Test the validation script using system dependencies instead of Conda.  This
# script is meant for automation (e.g. Docker), not a local/developer machine
# (except via Docker).

set -euo pipefail

: ${JDK:=21}
: ${MAVEN:=3.9.11}

main() {
    local -r source_dir="${1}"

    # Install all the dependencies we need for all the subprojects
    echo "JDK=${JDK}"
    echo "MAVEN=${MAVEN}"

    # When installing tzdata, don't block and wait for user input
    export DEBIAN_FRONTEND=noninteractive
    export TZ=Etc/UTC

    echo "::group::Install APT dependencies"
    apt update
    apt install -y \
        apt-transport-https \
        build-essential \
        ca-certificates \
        cmake \
        curl \
        dirmngr \
        git \
        gobject-introspection \
        gpg \
        libgirepository1.0-dev \
        libglib2.0-dev \
        libgmock-dev \
        libgtest-dev \
        libpq-dev \
        libsqlite3-dev \
        lsb-release \
        ninja-build \
        pkg-config \
        protobuf-compiler \
        python3 \
        python3-dev \
        python3-pip \
        python3-venv \
        r-base \
        ruby-full \
        software-properties-common \
        wget
    echo "::endgroup::"

    echo "::group::Install Java"
    echo "============================================================"
    echo "Installing Java ${JDK}..."
    echo "============================================================"
    wget -qO - https://packages.adoptium.net/artifactory/api/gpg/key/public | \
        gpg --dearmor | \
        tee /etc/apt/trusted.gpg.d/adoptium.gpg > /dev/null

    echo "deb https://packages.adoptium.net/artifactory/deb $(awk -F= '/^VERSION_CODENAME/{print$2}' /etc/os-release) main" | \
        tee /etc/apt/sources.list.d/adoptium.list

    apt update
    apt install -y temurin-${JDK}-jdk
    echo "::endgroup::"

    echo "::group::Install Arrow GLib"
    echo "============================================================"
    echo "Installing Arrow GLib..."
    echo "============================================================"
    wget https://packages.apache.org/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
    apt install -y -V ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb

    apt update
    apt install -y \
        libarrow-dev \
        libarrow-glib-dev
    echo "::endgroup::"

    # Install Maven
    echo "::group::Install Maven"
    echo "============================================================"
    echo "Installing Maven ${MAVEN}..."
    echo "============================================================"
    wget -O apache-maven.tar.gz https://dlcdn.apache.org/maven/maven-3/${MAVEN}/binaries/apache-maven-${MAVEN}-bin.tar.gz
    mkdir -p /opt/maven
    tar -C /opt/maven -xzvf apache-maven.tar.gz --strip-components=1
    export PATH=/opt/maven/bin:$PATH
    echo "::endgroup::"

    # Check if protoc is too old (Ubuntu 22.04).  If so, install it from
    # upstream instead.
    if ! protoc --version | \
            awk '{print $2}{print "3.15"}' | \
            sort --version-sort | \
            head -n1 | \
            grep -E '^3\.15.*$' >/dev/null ; then
        echo "::group::Install protoc"
        echo "============================================================"
        echo "Installing protoc..."
        echo "============================================================"
        echo "protoc is too old"

        wget -O protoc.zip https://github.com/protocolbuffers/protobuf/releases/download/v30.2/protoc-30.2-linux-x86_64.zip
        unzip -o protoc.zip -d /usr/local -x readme.txt
        echo "::endgroup::"
    fi

    # We run under Docker and this is necessary since the source dir is
    # typically mounted as a volume
    git config --global --add safe.directory "${source_dir}"

    echo "::group::Verify"
    "${source_dir}/dev/release/verify-release-candidate.sh"
    echo "::endgroup::"
}

main "$@"
