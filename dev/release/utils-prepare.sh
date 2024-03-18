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
      local conda_version=${base_version}
      local docs_version=${base_version}
      local py_version=${base_version}
      local r_version=${base_version}
      ;;
    snapshot)
      local version=${next_version}-SNAPSHOT
      local conda_version=${next_version}
      local docs_version="${next_version} (dev)"
      local py_version="${next_version}dev"
      local r_version="${base_version}.9000"
      ;;
  esac
  local major_version=${version%%.*}

  pushd "${ADBC_DIR}/c/"
  sed -i.bak -E "s/set\(ADBC_VERSION \".+\"\)/set(ADBC_VERSION \"${version}\")/g" cmake_modules/AdbcVersion.cmake
  rm cmake_modules/AdbcVersion.cmake.bak
  git add cmake_modules/AdbcVersion.cmake
  popd

  pushd "${ADBC_DIR}/ci/conda/"
  sed -i.bak -E "s/version: .+/version: ${conda_version}/g" meta.yaml
  rm meta.yaml.bak
  git add meta.yaml
  popd

  sed -i.bak -E "s/<version>.+</version>/<version>${version}</version>/" "${ADBC_DIR}/csharp/Directory.Build.props"
  rm "${ADBC_DIR}/csharp/Directory.Build.props.bak"
  git add "${ADBC_DIR}/csharp/Directory.Build.props"

  sed -i.bak -E "s/release = \".+\"/release = \"${docs_version}\"/g" "${ADBC_DIR}/docs/source/conf.py"
  rm "${ADBC_DIR}/docs/source/conf.py.bak"
  git add "${ADBC_DIR}/docs/source/conf.py"

  pushd "${ADBC_DIR}/java/"
  mvn versions:set "-DnewVersion=${version}" '-DoldVersion=*'
  find . -type f -name pom.xml.versionsBackup -delete
  sed -i.bak -E "s|<adbc\\.version>.+</adbc\\.version>|<adbc.version>${version}</adbc.version>|g" pom.xml
  rm pom.xml.bak
  git add "pom.xml" "**/pom.xml"
  popd

  sed -i.bak -E "s/version: '.+'/version: '${version}'/g" "${ADBC_DIR}/glib/meson.build"
  rm "${ADBC_DIR}/glib/meson.build.bak"
  git add "${ADBC_DIR}/glib/meson.build"

  sed -i.bak -E "s/version = \".+\"/version = \"${py_version}\"/g" "${ADBC_DIR}"/python/adbc_*/adbc_*/_static_version.py
  rm "${ADBC_DIR}"/python/adbc_*/adbc_*/_static_version.py.bak
  git add "${ADBC_DIR}"/python/adbc_*/adbc_*/_static_version.py

  sed -i.bak -E "s/VERSION = \".+\"/VERSION = \"${version}\"/g" "${ADBC_DIR}/ruby/lib/adbc/version.rb"
  rm "${ADBC_DIR}/ruby/lib/adbc/version.rb.bak"
  git add "${ADBC_DIR}/ruby/lib/adbc/version.rb"

  for desc_file in $(find "${ADBC_DIR}/r" -name DESCRIPTION); do
    sed -i.bak -E "s/Version:.*$/Version: ${r_version}/" "${desc_file}"
    rm "${desc_file}.bak"
    git add "${desc_file}"
  done

  sed -i.bak -E "s/^version = \".+\"/version = \"${version}\"/" "${ADBC_DIR}/rust/Cargo.toml"
  rm "${ADBC_DIR}/rust/Cargo.toml.bak"
  git add "${ADBC_DIR}/rust/Cargo.toml"

  if [ ${type} = "release" ]; then
    pushd "${ADBC_DIR}/ci/linux-packages"
    rake version:update VERSION=${version}
    git add debian*/changelog yum/*.spec.in
    popd
  else
    so_version() {
      local -r version=$1
      local -r major_version=$(echo $version | sed -E -e 's/^([0-9]+)\.[0-9]+\.[0-9]+$/\1/')
      local -r minor_version=$(echo $version | sed -E -e 's/^[0-9]+\.([0-9]+)\.[0-9]+$/\1/')
      printf "%0d%02d" ${major_version} ${minor_version}
    }
    local -r deb_lib_suffix=$(so_version ${base_version})
    local -r next_deb_lib_suffix=$(so_version ${next_version})
    pushd "${ADBC_DIR}/ci/linux-packages"
    if [ "${deb_lib_suffix}" != "${next_deb_lib_suffix}" ]; then
      for target in debian*/lib*${deb_lib_suffix}.install; do
        git mv \
          ${target} \
          $(echo ${target} | sed -e "s/${deb_lib_suffix}/${next_deb_lib_suffix}/")
      done
      sed -i.bak -E \
        -e "s/(lib[-a-z]*)${deb_lib_suffix}/\\1${next_deb_lib_suffix}/g" \
        debian*/control*
      rm -f debian*/control*.bak
      git add debian*/control*
    fi
    local -r base_major_version=$(echo ${base_version} | sed -E -e 's/^([0-9]+)\.[0-9]+\.[0-9]+$/\1/')
    local -r next_major_version=$(echo ${next_version} | sed -E -e 's/^([0-9]+)\.[0-9]+\.[0-9]+$/\1/')
    if [ "${base_major_version}" != "${next_major_version}" ]; then
      for target in debian*/libadbc-glib${base_major_version}.install; do
        git mv \
          ${target} \
          $(echo ${target} | sed -e "s/${base_major_version}/${next_major_version}/")
      done
    fi
    popd
  fi
}

set_resolved_issues() {
    # TODO: this needs to work with open milestones
    local -r version="${1}"
    local -r milestone_info=$(gh api \
                                 /repos/apache/arrow-adbc/milestones \
                                 -X GET \
                                 -F state=all \
                                 --jq ".[] | select(.title | test(\"ADBC Libraries ${version}\$\"))")
    local -r milestone_number=$(echo "${milestone_info}" | jq -r '.number')

    local -r graphql_query="query {
    repository(owner: \"apache\", name: \"arrow-adbc\") {
        milestone(number: ${milestone_number}) {
            issues(states: CLOSED) {
                totalCount
            }
        }
    }
}
"

    export MILESTONE_URL=$(echo "${milestone_info}" | jq -r '.html_url')
    export RESOLVED_ISSUES=$(gh api graphql \
                            -f query="${graphql_query}" \
                            --jq '.data.repository.milestone.issues.totalCount')
}
