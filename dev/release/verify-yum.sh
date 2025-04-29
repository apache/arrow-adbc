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
local_prefix="${SOURCE_DIR}/../../ci/linux-packages"

artifactory_base_url="https://packages.apache.org/artifactory/arrow"

distribution=$(. /etc/os-release && echo "${ID}")
distribution_version=$(. /etc/os-release && echo "${VERSION_ID}" | grep -o "^[0-9]*")
repository_version="${distribution_version}"

ruby_devel_packages=(ruby-devel)
install_command="dnf install -y --enablerepo=crb"
uninstall_command="dnf remove -y"
clean_command="dnf clean"
info_command="dnf info --enablerepo=crb"

echo "::group::Prepare repository"

case "${distribution}-${distribution_version}" in
  almalinux-8)
    distribution_prefix="almalinux"
    ruby_devel_packages+=(redhat-rpm-config)
    install_command="dnf install -y --enablerepo=powertools"
    info_command="dnf info --enablerepo=powertools"
    ;;
  almalinux-*)
    distribution_prefix="almalinux"
    ruby_devel_packages+=(redhat-rpm-config)
    ;;
esac

${install_command} \
    ${artifactory_base_url}/${distribution_prefix}/${repository_version}/apache-arrow-release-latest.rpm

if [ "${TYPE}" = "local" ]; then
  case "${VERSION}" in
    *-dev*)
      package_version="$(echo "${VERSION}" | sed -e 's/-dev\(.*\)$/-0.dev\1/g')"
      ;;
    *-rc*)
      package_version="$(echo "${VERSION}" | sed -e 's/-rc.*$//g')"
      package_version+="-1"
      ;;
    *)
      package_version="${VERSION}-1"
      ;;
  esac
  case "${distribution}" in
    almalinux)
      package_version+=".el${distribution_version}"
      ;;
  esac
else
  package_version="${VERSION}"
fi

if [ "${TYPE}" = "local" ]; then
  sed \
    -e "s,^\\[apache-arrow-,\\[apache-adbc-,g" \
    -e "s,baseurl=https://packages\.apache\.org/artifactory/arrow/,baseurl=file://${local_prefix}/yum/repositories/,g" \
    -e "s,RPM-GPG-KEY-Apache-Arrow,RPM-GPG-KEY-Apache-ADBC,g" \
    /etc/yum.repos.d/Apache-Arrow.repo > \
    /etc/yum.repos.d/Apache-ADBC.repo
  keys="${local_prefix}/KEYS"
  if [ -f "${keys}" ]; then
    cp "${keys}" /etc/pki/rpm-gpg/RPM-GPG-KEY-Apache-ADBC
  fi
else
  case "${TYPE}" in
    rc|staging-rc|staging-release)
      suffix=${TYPE%-release}
      sed \
        -i"" \
        -e "s,/almalinux/,/almalinux-${suffix}/,g" \
        -e "s,/centos/,/centos-${suffix}/,g" \
        -e "s,/amazon-linux/,/amazon-linux-${suffix}/,g" \
        /etc/yum.repos.d/Apache-Arrow.repo
      ;;
  esac
fi

echo "::endgroup::"


echo "::group::Test ADBC Driver Manager"
${install_command} --enablerepo=epel adbc-driver-manager-devel-${package_version}
${install_command} \
  cmake \
  gcc \
  make \
  pkg-config
# TODO
# mkdir -p build
# cp -a "${TOP_SOURCE_DIR}/cpp/examples/minimal_build" build/
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
${install_command} --enablerepo=epel adbc-driver-postgresql-devel-${package_version}
echo "::endgroup::"

echo "::group::Test ADBC SQLite Driver"
${install_command} --enablerepo=epel adbc-driver-sqlite-devel-${package_version}
echo "::endgroup::"

echo "::group::Test ADBC Flight SQL Driver"
${install_command} --enablerepo=epel adbc-driver-flightsql-devel-${package_version}
echo "::endgroup::"

echo "::group::Test ADBC Snowflake Driver"
${install_command} --enablerepo=epel adbc-driver-snowflake-devel-${package_version}
echo "::endgroup::"

echo "::group::Test ADBC GLib"
export G_DEBUG=fatal-warnings

${install_command} --enablerepo=epel adbc-glib-devel-${package_version}
${install_command} --enablerepo=epel adbc-glib-doc-${package_version}

${install_command} "${ruby_devel_packages[@]}"
gem install gobject-introspection
ruby -r gi -e "p GI.load('ADBC')"
echo "::endgroup::"

echo "::group::Test ADBC Arrow GLib"

${install_command} --enablerepo=epel adbc-arrow-glib-devel-${package_version}
${install_command} --enablerepo=epel adbc-arrow-glib-doc-${package_version}

ruby -r gi -e "p GI.load('ADBCArrow')"
echo "::endgroup::"
