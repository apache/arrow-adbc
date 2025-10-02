#!/usr/bin/env bash
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

set -exu

if [ $# -lt 2 ]; then
  echo "Usage: $0 VERSION rc"
  echo "       $0 VERSION staging-rc"
  echo "       $0 VERSION release"
  echo "       $0 VERSION staging-release"
  echo "       $0 VERSION local"
  echo " e.g.: $0 1.0.0 rc                # Verify 1.0.0 RC"
  echo " e.g.: $0 1.0.0 staging-rc        # Verify 1.0.0 RC on staging"
  echo " e.g.: $0 1.0.0 release           # Verify 1.0.0"
  echo " e.g.: $0 1.0.0 staging-release   # Verify 1.0.0 on staging"
  echo " e.g.: $0 1.0.0-dev20210203 local # Verify 1.0.0-dev20210203 on local"
  exit 1
fi

VERSION="$1"
TYPE="$2"

SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TOP_SOURCE_DIR="${SOURCE_DIR}/../.."
local_prefix="${TOP_SOURCE_DIR}/ci/linux-packages"


echo "::group::Prepare repository"

export DEBIAN_FRONTEND=noninteractive

retry() {
  local n_tries=2
  while [ ${n_tries} -gt 0 ]; do
    if "$@"; then
      return
    fi
    n_tries=$((${n_tries} - 1))
  done
  "$@"
}

APT_UPDATE="retry apt update --error-on=any"
APT_INSTALL="retry apt install -y -V --no-install-recommends"

${APT_UPDATE}
${APT_INSTALL} \
  ca-certificates \
  curl \
  lsb-release

code_name="$(lsb_release --codename --short)"
distribution="$(lsb_release --id --short | tr 'A-Z' 'a-z')"
artifactory_base_url="https://packages.apache.org/artifactory/arrow/${distribution}"
case "${TYPE}" in
  rc|staging-rc|staging-release)
    suffix=${TYPE%-release}
    artifactory_base_url+="-${suffix}"
    ;;
esac

case "${distribution}-${code_name}" in
  debian-*)
    sed \
      -i"" \
      -e "s/ main$/ main contrib non-free/g" \
      /etc/apt/sources.list.d/debian.sources
    ;;
esac

curl \
    --fail \
    --location \
    --remote-name \
     https://packages.apache.org/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
${APT_INSTALL} ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb

if [ "${TYPE}" = "local" ]; then
  case "${VERSION}" in
    *-dev*)
      package_version="$(echo "${VERSION}" | sed -e 's/-dev\(.*\)$/~dev\1/g')"
      ;;
    *-rc*)
      package_version="$(echo "${VERSION}" | sed -e 's/-rc.*$//g')"
      ;;
    *)
      package_version="${VERSION}"
      ;;
  esac
  package_version+="-1"
else
  package_version="${VERSION}-1"
fi

if [ "${TYPE}" = "local" ]; then
  sed \
    -e "s,^URIs: .*$,URIs: file://${local_prefix}/apt/repositories/${distribution},g" \
    -e "s,^Signed-By: .*$,Signed-By: /usr/share/keyrings/apache-arrow-adbc-apt-source.asc,g" \
    /etc/apt/sources.list.d/apache-arrow.sources |
      tee /etc/apt/sources.list.d/apache-arrow-adbc.sources
  keys="${local_prefix}/KEYS"
  if [ -f "${keys}" ]; then
    gpg \
      --no-default-keyring \
      --keyring /tmp/apache-arrow-adbc-apt-source.kbx \
      --import "${keys}"
    gpg \
      --no-default-keyring \
      --keyring /tmp/apache-arrow-adbc-apt-source.kbx \
      --armor \
      --export > /usr/share/keyrings/apache-arrow-adbc-apt-source.asc
  fi
else
  case "${TYPE}" in
    rc|staging-rc|staging-release)
      suffix=${TYPE%-release}
      sed \
        -i"" \
        -e "s,^URIs: \\(.*\\)/,URIs: \\1-${suffix}/,g" \
        /etc/apt/sources.list.d/apache-arrow.sources
      ;;
  esac
fi

${APT_UPDATE}

echo "::endgroup::"


echo "::group::Test ADBC Driver Manager"
${APT_INSTALL} libadbc-driver-manager-dev=${package_version}
required_packages=()
required_packages+=(cmake)
required_packages+=(gcc)
required_packages+=(libc6-dev)
required_packages+=(make)
required_packages+=(pkg-config)
${APT_INSTALL} ${required_packages[@]}
# TODO
# mkdir -p build
# cp -a ${TOP_SOURCE_DIR}/c/driver_manager/example build/
# pushd build/example
# cmake .
# make -j$(nproc)
# ./adbc-driver-manager-example
# cc \
#   -o adbc-driver-manager \
#   adbc-driver-manager.c \
#   $(pkg-config --cflags --libs adbc-driver-manager)
# ./adbc-driver-manager-example
# popd
echo "::endgroup::"


echo "::group::Test ADBC PostgreSQL Driver"
${APT_INSTALL} libadbc-driver-postgresql-dev=${package_version}
echo "::endgroup::"


echo "::group::Test ADBC SQLite Driver"
${APT_INSTALL} libadbc-driver-sqlite-dev=${package_version}
echo "::endgroup::"


echo "::group::Test ADBC Flight SQL Driver"
${APT_INSTALL} libadbc-driver-flightsql-dev=${package_version}
echo "::endgroup::"

echo "::group::Test ADBC Snowflake Driver"
${APT_INSTALL} libadbc-driver-snowflake-dev=${package_version}
echo "::endgroup::"

echo "::group::Test ADBC GLib"
export G_DEBUG=fatal-warnings

${APT_INSTALL} libadbc-glib-dev=${package_version}
${APT_INSTALL} libadbc-glib-doc=${package_version}

${APT_INSTALL} ruby-dev rubygems-integration
gem install gobject-introspection
ruby -r gi -e "p GI.load('ADBC')"
echo "::endgroup::"

echo "::group::Test ADBC Arrow GLib"

${APT_INSTALL} libadbc-arrow-glib-dev=${package_version}
${APT_INSTALL} libadbc-arrow-glib-doc=${package_version}

ruby -r gi -e "p GI.load('ADBCArrow')"
echo "::endgroup::"
