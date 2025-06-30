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

set -ex

arch=${1}
source_dir=${2}
build_dir=${3}
script_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

source "${script_dir}/python_util.sh"

function check_visibility {
    if [[ $(uname) != "Linux" ]]; then
       return 0
    fi
    nm --demangle --dynamic $1 > nm_arrow.log

    # Filter out Arrow symbols and see if anything remains.
    # '_init' and '_fini' symbols may or not be present, we don't care.
    # (note we must ignore the grep exit status when no match is found)
    grep ' T ' nm_arrow.log | grep -v -E '(Adbc|DriverInit|\b_init\b|\b_fini\b)' | cat - > visible_symbols.log

    if [[ -f visible_symbols.log && `cat visible_symbols.log | wc -l` -eq 0 ]]; then
        echo "No unexpected symbols exported by $1"
    else
        echo "== Unexpected symbols exported by $1 =="
        cat visible_symbols.log
        echo "================================================"

        exit 1
    fi

    # Also check the max glibc/glibcxx version, to avoid accidentally bumping
    # our manylinux requirement
    # See https://peps.python.org/pep-0599/#the-manylinux2014-policy
    local -r glibc_max=2.17
    local -r glibcxx_max=3.4.19
    local -r glibc_requirement=$(grep -Eo 'GLIBC_\S+' nm_arrow.log | awk -F_ '{print $2}' | sort --version-sort -u | tail -n1)
    local -r glibc_maxver=$(echo -e "${glibc_requirement}\n${glibc_max}" | sort --version-sort | tail -n1)
    local -r glibcxx_requirement=$(grep -Eo 'GLIBCXX_\S+' nm_arrow.log | awk -F_ '{print $2}' | sort --version-sort -u | tail -n1)
    local -r glibcxx_maxver=$(echo -e "${glibcxx_requirement}\n${glibcxx_max}" | sort --version-sort | tail -n1)
    if [[ "${glibc_maxver}" != "2.17" ]]; then
        echo "== glibc check failed for $1 =="
        echo "Expected GLIBC_${glibc_max} but found GLIBC_${glibc_requirement}"
        exit 1
    elif [[ "${glibcxx_maxver}" != "3.4.19" ]]; then
        echo "== glibc check failed for $1 =="
        echo "Expected GLIBCXX_${glibcxx_max} but found GLIBCXX_${glibcxx_requirement}"
        exit 1
    fi
}

echo "=== Set up platform variables ==="
setup_build_vars "${arch}"

echo "=== Building C/C++ driver components ==="
# Sets ADBC_POSTGRESQL_LIBRARY, ADBC_SQLITE_LIBRARY
build_drivers "${source_dir}" "${build_dir}"

# Check that we don't expose any unwanted symbols
check_visibility $ADBC_BIGQUERY_LIBRARY
check_visibility $ADBC_FLIGHTSQL_LIBRARY
check_visibility $ADBC_POSTGRESQL_LIBRARY
check_visibility $ADBC_SQLITE_LIBRARY
check_visibility $ADBC_SNOWFLAKE_LIBRARY
