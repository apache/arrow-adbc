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
    grep ' T ' nm_arrow.log | grep -v -E '(Adbc|\b_init\b|\b_fini\b)' | cat - > visible_symbols.log

    if [[ -f visible_symbols.log && `cat visible_symbols.log | wc -l` -eq 0 ]]; then
        return 0
    else
        echo "== Unexpected symbols exported by $1 =="
        cat visible_symbols.log
        echo "================================================"

        exit 1
    fi
}

function check_wheels {
    if [[ $(uname) == "Linux" ]]; then
        echo "=== Tag $component wheel with manylinux${MANYLINUX_VERSION} ==="
        auditwheel repair "$@" -L . -w repaired_wheels
    else # macOS
        echo "=== Tag $component wheel with macOS ==="
        delocate-wheel -v -k -w repaired_wheels "$@"
    fi
}

echo "=== Set up platform variables ==="
setup_build_vars "${arch}"

# XXX: when we manually retag the wheel, we have to use the right arch
# tag accounting for cross-compiling, hence the replacements
PLAT_NAME=$(python -c "import sysconfig; print(sysconfig.get_platform()\
    .replace('-x86_64', '-${PYTHON_ARCH}')\
    .replace('-arm64', '-${PYTHON_ARCH}')\
    .replace('-universal2', '-${PYTHON_ARCH}'))")
if [[ "${arch}" = "arm64v8" && "$(uname)" = "Darwin" ]]; then
   # Manually override the tag in this case - CI will naively generate
   # "macosx_10_9_arm64" but this isn't a 'real' tag because the first
   # version of macOS supporting AArch64 was macOS 11 Big Sur
   PLAT_NAME="macosx_11_0_arm64"
fi

echo "=== Building C/C++ driver components ==="
# Sets ADBC_POSTGRES_LIBRARY, ADBC_SQLITE_LIBRARY
build_drivers "${source_dir}" "${build_dir}"

# Check that we don't expose any unwanted symbols
check_visibility $ADBC_POSTGRES_LIBRARY
check_visibility $ADBC_SQLITE_LIBRARY

# https://github.com/pypa/pip/issues/7555
# Get the latest pip so we have in-tree-build by default
python -m pip install --upgrade pip auditwheel cibuildwheel delocate setuptools wheel

for component in $COMPONENTS; do
    pushd ${source_dir}/python/$component

    echo "=== Clean build artifacts ==="
    rm -rf ./build ./dist ./repaired_wheels ./$component/*.so ./$component/*.so.*

    echo "=== Check $component version ==="
    python $component/_version.py

    echo "=== Building $component wheel ==="
    # First, create an sdist, which 1) bundles the C++ sources and 2)
    # embeds the git tag.  cibuildwheel may copy into a Docker
    # container during build, but it only copies the package
    # directory, which omits the C++ sources and .git directory,
    # causing the build to fail.
    python setup.py sdist
    if [[ "$component" = "adbc_driver_manager" ]]; then
        python -m cibuildwheel --output-dir repaired_wheels/ dist/$component-*.tar.gz
    else
        python -m pip wheel -w dist -vvv .

        # Retag the wheel
        python "${script_dir}/python_wheel_fix_tag.py" --plat-name="${PLAT_NAME}" dist/$component-*.whl

        check_wheels dist/$component-*.whl
    fi

    popd
done
