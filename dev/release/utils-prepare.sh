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

ADBC_DIR="${SOURCE_DIR}/../.."

update_versions() {
  local base_version=$1
  local next_version=$2
  local type=$3

  case ${type} in
    release)
      local version=${base_version}
      local docs_version=${base_version}
      ;;
    snapshot)
      local version=${next_version}-SNAPSHOT
      local docs_version="${next_version} (dev)"
      ;;
  esac
  local major_version=${version%%.*}

  pushd "${ADBC_DIR}/c/"
  sed -i.bak -E "s/set\(ADBC_VERSION \".+\"\)/set(ADBC_VERSION \"${version}\")/g" cmake_modules/AdbcDefines.cmake
  rm cmake_modules/AdbcDefines.cmake.bak
  git add cmake_modules/AdbcDefines.cmake
  popd

  sed -i.bak -E "s/release = \".+\"/release = \"${docs_version}\"/g" "${ADBC_DIR}/docs/source/conf.py"
  rm "${ADBC_DIR}/docs/source/conf.py.bak"
  git add "${ADBC_DIR}/docs/source/conf.py"

  pushd "${ADBC_DIR}/java/"
  mvn versions:set "-DnewVersion=${version}"
  find . -type f -name pom.xml.versionsBackup -delete
  sed -i.bak -E "s|<adbc.version>.+</adbc.version>|<adbc.version>${version}</adbc.version>|g" pom.xml
  rm pom.xml.bak
  git add "pom.xml" "**/pom.xml"
  popd

  sed -i.bak -E "s/version: '.+'/release = '${version}'/g" "${ADBC_DIR}/glib/meson.build"
  rm "${ADBC_DIR}/glib/meson.build.bak"
  git add "${ADBC_DIR}/glib/meson.build"

  sed -i.bak -E "s/VERSION = \".+\"/VERSION = \"${version}\"/g" "${ADBC_DIR}/ruby/lib/adbc/version.rb"
  rm "${ADBC_DIR}/ruby/lib/adbc/version.rb.bak"
  git add "${ADBC_DIR}/ruby/lib/adbc/version.rb"
}
