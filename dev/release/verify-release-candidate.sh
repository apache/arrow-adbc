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
#

# Requirements
# - Ruby >= 3.2
# - Maven >= 3.3.9
# - JDK >= 11
# - gcc >= 4.8
# - Go >= 1.21
# - Docker
#
# To reuse build artifacts between runs set ARROW_TMPDIR environment variable to
# a directory where the temporary files should be placed to, note that this
# directory is not cleaned up automatically.

set -eo pipefail

if [ ${VERBOSE:-0} -gt 0 ]; then
  set -x
fi

case $# in
  0) VERSION="HEAD"
     SOURCE_KIND="local"
     TEST_BINARIES=0
     ;;
  1) VERSION="$1"
     SOURCE_KIND="git"
     TEST_BINARIES=0
     ;;
  2) VERSION="$1"
     RC_NUMBER="$2"
     SOURCE_KIND="tarball"
     ;;
  *) echo "Usage:"
     echo "  Verify release candidate:"
     echo "    $0 X.Y.Z RC_NUMBER"
     echo "  Verify only the source distribution:"
     echo "    TEST_DEFAULT=0 TEST_SOURCE=1 $0 X.Y.Z RC_NUMBER"
     echo "  Verify only the binary distributions:"
     echo "    TEST_DEFAULT=0 TEST_BINARIES=1 $0 X.Y.Z RC_NUMBER"
     echo "  Verify only the wheels:"
     echo "    TEST_DEFAULT=0 TEST_WHEELS=1 $0 X.Y.Z RC_NUMBER"
     echo ""
     echo "  Run the source verification tasks on a remote git revision:"
     echo "    $0 GIT-REF"
     echo "  Run the source verification tasks on this arrow checkout:"
     echo "    $0"
     exit 1
     ;;
esac

# Note that these point to the current verify-release-candidate.sh directories
# which is different from the ADBC_SOURCE_DIR set in ensure_source_directory()
SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"
ADBC_DIR="$(cd "${SOURCE_DIR}/../.." && pwd)"

: ${ARROW_REPOSITORY:="apache/arrow"}
: ${SOURCE_REPOSITORY:="apache/arrow-adbc"}

show_header() {
  echo ""
  printf '=%.0s' $(seq ${#1}); printf '\n'
  echo "${1}"
  printf '=%.0s' $(seq ${#1}); printf '\n'
}

show_info() {
  echo "└ ${1}"
}

ARROW_DIST_URL='https://dist.apache.org/repos/dist/dev/arrow'

download_dist_file() {
  if [[ -n "${VERIFICATION_MOCK_DIST_DIR}" ]]; then
    cp "${VERIFICATION_MOCK_DIST_DIR}/$1" .
  else
    curl \
      --silent \
      --show-error \
      --fail \
      --location \
      --remote-name $ARROW_DIST_URL/$1
  fi
}

download_rc_file() {
  download_dist_file apache-arrow-adbc-${VERSION}-rc${RC_NUMBER}/$1
}

import_gpg_keys() {
  if [ "${GPGKEYS_ALREADY_IMPORTED:-0}" -gt 0 ]; then
    return 0
  fi
  download_dist_file KEYS
  gpg --import KEYS

  GPGKEYS_ALREADY_IMPORTED=1
}

if type shasum >/dev/null 2>&1; then
  sha512_verify="shasum -a 512 -c"
else
  sha512_verify="sha512sum -c"
fi

fetch_archive() {
  import_gpg_keys

  local dist_name=$1
  download_rc_file ${dist_name}.tar.gz
  download_rc_file ${dist_name}.tar.gz.asc
  download_rc_file ${dist_name}.tar.gz.sha512
  gpg --verify ${dist_name}.tar.gz.asc ${dist_name}.tar.gz
  ${sha512_verify} ${dist_name}.tar.gz.sha512
}

verify_dir_artifact_signatures() {
  import_gpg_keys

  # verify the signature and the checksums of each artifact
  find $1 -name '*.asc' | while read sigfile; do
    artifact=${sigfile/.asc/}
    gpg --verify $sigfile $artifact || exit 1

    # go into the directory because the checksum files contain only the
    # basename of the artifact
    pushd $(dirname $artifact) >/dev/null
    base_artifact=$(basename $artifact)
    ${sha512_verify} $base_artifact.sha512 || exit 1
    popd >/dev/null
  done
}

test_binary() {
  show_header "Testing binary artifacts"
  maybe_setup_conda || exit 1

  verify_dir_artifact_signatures ${BINARY_DIR}
}

test_apt() {
  show_header "Testing APT packages"

  if ! type docker > /dev/null 2>&1; then
    show_info "Skip because Docker isn't installed"
    return 0
  fi

  local verify_type=rc
  if [ "${TEST_STAGING:-0}" -gt 0 ]; then
    verify_type=staging-${verify_type}
  fi
  for target in "debian:bookworm" \
                "ubuntu:jammy"; do \
    show_info "Verifying ${target}..."
    if ! docker run \
	   --rm \
           --security-opt="seccomp=unconfined" \
           --volume "${ADBC_DIR}":/host:delegated \
           "${target}" \
           /host/dev/release/verify-apt.sh \
           "${VERSION}" \
           "${verify_type}"; then
      echo "Failed to verify the APT repository for ${target}"
      exit 1
    fi
  done
}

test_yum() {
  show_header "Testing Yum packages"

  if ! type docker > /dev/null 2>&1; then
    show_info "Skip because Docker isn't installed"
    return 0
  fi

  local verify_type=rc
  if [ "${TEST_STAGING:-0}" -gt 0 ]; then
    verify_type=staging-${verify_type}
  fi
  for target in "almalinux:9" \
                "almalinux:8"; do
    show_info "Verifying ${target}..."
    if ! docker run \
           --rm \
           --security-opt="seccomp=unconfined" \
           --volume "${ADBC_DIR}":/host:delegated \
           "${target}" \
           /host/dev/release/verify-yum.sh \
           "${VERSION}" \
           "${verify_type}"; then
      echo "Failed to verify the Yum repository for ${target}"
      exit 1
    fi
  done
}

setup_tempdir() {
  cleanup() {
    # Go modules are installed with 0444.
    chmod -R u+w "${ARROW_TMPDIR}"
    if [ "${TEST_SUCCESS}" = "yes" ]; then
      rm -fr "${ARROW_TMPDIR}"
    else
      echo "Failed to verify release candidate. See ${ARROW_TMPDIR} for details."
    fi
  }

  show_header "Creating temporary directory"

  if [ -z "${ARROW_TMPDIR}" ]; then
    # clean up automatically if ARROW_TMPDIR is not defined
    ARROW_TMPDIR=$(mktemp -d -t "arrow-adbc-${VERSION}.XXXXX")
    trap cleanup EXIT
  else
    # don't clean up automatically
    mkdir -p "${ARROW_TMPDIR}"
  fi

  echo "Working in sandbox ${ARROW_TMPDIR}"
}

install_dotnet() {
  # Install C# if doesn't already exist
  if [ "${DOTNET_ALREADY_INSTALLED:-0}" -gt 0 ]; then
    show_info ".NET already installed $(which csharp) (.NET $(dotnet --version))"
    return 0
  fi

  if command -v dotnet; then
    show_info "Found $(dotnet --version) at $(which dotnet)"

    if dotnet --version | grep --quiet --fixed-string 8.0; then
        local csharp_bin=$(dirname $(which dotnet))
        show_info "Found C# at $(which csharp) (.NET $(dotnet --version))"
        DOTNET_ALREADY_INSTALLED=1
        return 0
    fi
  fi

  show_info "dotnet found but it is the wrong version or dotnet not found"

  local csharp_bin=${ARROW_TMPDIR}/csharp/bin
  local dotnet_version=8.0.204
  local dotnet_platform=
  case "$(uname)" in
      Linux)
          dotnet_platform=linux
          ;;
      Darwin)
          dotnet_platform=macos
          ;;
  esac
  local dotnet_download_thank_you_url=https://dotnet.microsoft.com/download/thank-you/dotnet-sdk-${dotnet_version}-${dotnet_platform}-x64-binaries
  show_info "Getting .NET download URL from ${dotnet_download_thank_you_url}"
  curl --fail -L -o "${ARROW_TMPDIR}/dotnetdownload.html" "${dotnet_download_thank_you_url}"
  local dotnet_download_url=$(grep 'directLink' "${ARROW_TMPDIR}/dotnetdownload.html" | \
                                  grep -E -o 'https://builds.dotnet[^"]+' | \
                                  head -n1)
  if [ -z "${dotnet_download_url}" ]; then
    echo "Failed to get .NET download URL from ${dotnet_download_thank_you_url}"
    exit 1
  fi
  show_info "Downloading .NET from ${dotnet_download_url}"
  mkdir -p ${csharp_bin}
  curl -sL ${dotnet_download_url} | \
      tar xzf - -C ${csharp_bin}
  PATH=${csharp_bin}:${PATH}
  show_info "Installed C# at $(which csharp) (.NET $(dotnet --version))"

  DOTNET_ALREADY_INSTALLED=1
}

install_go() {
  # Install go
  if [ "${GO_ALREADY_INSTALLED:-0}" -gt 0 ]; then
    show_info "$(go version) already installed at $(which go)"
    return 0
  fi

  if command -v go > /dev/null; then
    show_info "Found $(go version) at $(command -v go)"
    export GOPATH=${ARROW_TMPDIR}/gopath
    mkdir -p $GOPATH
    return 0
  fi

  local prefix=${ARROW_TMPDIR}/go
  mkdir -p $prefix

  if [ -f "${prefix}/go/bin/go" ]; then
    export GOROOT=${prefix}/go
    export GOPATH=${prefix}/gopath
    export PATH=$GOROOT/bin:$GOPATH/bin:$PATH
    show_info "Found $(go version) at ${prefix}/go/bin/go"
    GO_ALREADY_INSTALLED=1
    return 0
  fi

  local version=1.24.2
  show_info "Installing go version ${version}..."

  local arch="$(uname -m)"
  if [ "$arch" == "x86_64" ]; then
    arch=amd64
  elif [ "$arch" == "aarch64" ]; then
    arch=arm64
  fi

  if [ "$(uname)" == "Darwin" ]; then
    local os=darwin
  else
    local os=linux
  fi

  local archive="go${version}.${os}-${arch}.tar.gz"
  curl -sLO https://go.dev/dl/$archive

  tar -xzf $archive -C $prefix
  rm -f $archive

  export GOROOT=${prefix}/go
  export GOPATH=${prefix}/gopath
  export PATH=$GOROOT/bin:$GOPATH/bin:$PATH

  mkdir -p $GOPATH
  show_info "$(go version) installed at $(which go)"

  GO_ALREADY_INSTALLED=1
}

install_rust() {
  if [ "${RUST_ALREADY_INSTALLED:-0}" -gt 0 ]; then
    show_info "Rust already installed at $(command -v cargo)"
    show_info "$(cargo --version)"
    return 0
  fi

  if [[ -f ${ARROW_TMPDIR}/cargo/env ]]; then
    source ${ARROW_TMPDIR}/cargo/env
    rustup default stable
    show_info "$(cargo version) installed at $(command -v cargo)"
    RUST_ALREADY_INSTALLED=1
    return 0
  fi

  if command -v cargo > /dev/null; then
    show_info "Found $(cargo version) at $(command -v cargo)"
    RUST_ALREADY_INSTALLED=1
    return 0
  fi

  show_info "Installing Rust..."
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs |\
      env \
          RUSTUP_HOME=${ARROW_TMPDIR}/rustup \
          CARGO_HOME=${ARROW_TMPDIR}/cargo \
          sh -s -- \
          --default-toolchain stable \
          --no-modify-path \
          -y

  source ${ARROW_TMPDIR}/cargo/env
  rustup default stable

  show_info "$(cargo version) installed at $(command -v cargo)"

  RUST_ALREADY_INSTALLED=1
}

install_conda() {
  # Setup short-lived miniconda for Python and integration tests
  show_info "Ensuring that Conda is installed..."
  local prefix=$ARROW_TMPDIR/miniforge

  # Setup miniconda only if the directory doesn't exist yet
  if [ "${CONDA_ALREADY_INSTALLED:-0}" -eq 0 ]; then
    if [ ! -d "${prefix}" ]; then
      show_info "Installing miniconda at ${prefix}..."
      local arch=$(uname -m)
      local platform=$(uname)
      local url="https://github.com/conda-forge/miniforge/releases/latest/download/miniforge3-${platform}-${arch}.sh"
      curl -sL -o miniconda.sh $url
      bash miniconda.sh -b -p $prefix
      rm -f miniconda.sh
    else
      show_info "Miniconda already installed at ${prefix}"
    fi
  else
    show_info "Conda installed at ${prefix}"
  fi
  CONDA_ALREADY_INSTALLED=1

  # Creating a separate conda environment
  . $prefix/etc/profile.d/conda.sh
  conda activate base
}

maybe_setup_conda() {
  # Optionally setup conda environment with the passed dependencies
  local env="conda-${CONDA_ENV:-source}"
  local pyver=${PYTHON_VERSION:-3}

  if [ "${USE_CONDA}" -gt 0 ]; then
    show_info "Configuring Conda environment..."

    # Deactivate previous env
    if [ ! -z ${CONDA_PREFIX} ]; then
      conda deactivate || :
    fi
    # Ensure that conda is installed
    install_conda
    # Create environment
    if ! conda env list | cut -d" " -f 1 | grep $env; then
      mamba create -y -n $env python=${pyver}
    fi
    # Install dependencies
    if [ $# -gt 0 ]; then
      mamba install -y -n $env $@
    fi
    # Activate the environment
    conda activate $env
  elif [ ! -z ${CONDA_PREFIX} ]; then
    echo "Conda environment is active despite that USE_CONDA is set to 0."
    echo "Deactivate the environment using \`conda deactivate\` before running the verification script."
    return 1
  fi
}

maybe_setup_dotnet() {
  show_info "Ensuring that .NET is installed..."
  if [ "${USE_CONDA}" -eq 0 ]; then
    install_dotnet
  fi
}

maybe_setup_virtualenv() {
  # Optionally setup pip virtualenv with the passed dependencies
  local env="venv-${VENV_ENV:-source}"
  local pyver=${PYTHON_VERSION:-3}
  local python=${PYTHON:-"python${pyver}"}
  local virtualenv="${ARROW_TMPDIR}/${env}"
  local skip_missing_python=${SKIP_MISSING_PYTHON:-0}

  if [ "${USE_CONDA}" -eq 0 ]; then
    show_info "Configuring Python ${pyver} virtualenv..."

    if [ ! -z ${CONDA_PREFIX} ]; then
      echo "Conda environment is active despite that USE_CONDA is set to 0."
      echo "Deactivate the environment before running the verification script."
      return 1
    fi
    # Deactivate previous env
    if command -v deactivate &> /dev/null; then
      deactivate
    fi
    # Check that python interpreter exists
    if ! command -v "${python}" &> /dev/null; then
      echo "Couldn't locate python interpreter with version ${pyver}"
      echo "Call the script with USE_CONDA=1 to test all of the python versions."
      return 1
    else
      show_info "Found interpreter $($python --version): $(which $python)"
    fi
    # Create environment
    if [ ! -d "${virtualenv}" ]; then
      show_info "Creating python virtualenv at ${virtualenv}..."
      $python -m venv ${virtualenv}
      # Activate the environment
      source "${virtualenv}/bin/activate"
      # Upgrade pip and setuptools
      pip install -U pip setuptools
    else
      show_info "Using already created virtualenv at ${virtualenv}"
      # Activate the environment
      source "${virtualenv}/bin/activate"
    fi
    # Install dependencies
    if [ $# -gt 0 ]; then
      show_info "Installed pip packages $@..."
      pip install "$@"
    fi
  fi
}

maybe_setup_go() {
  show_info "Ensuring that Go is installed..."
  if [ "${USE_CONDA}" -eq 0 ]; then
    install_go
  fi
}

maybe_setup_rust() {
  show_info "Ensuring that Rust is installed..."
  if [ "${USE_CONDA}" -eq 0 ]; then
    install_rust
  fi
}

test_cpp() {
  show_header "Build, install and test C++ libraries"

  # Build and test C++
  maybe_setup_go
  maybe_setup_conda \
    --file ci/conda_env_cpp.txt \
    compilers \
    go python || exit 1

  if [ "${USE_CONDA}" -gt 0 ]; then
    export CMAKE_PREFIX_PATH="${CONDA_BACKUP_CMAKE_PREFIX_PATH}:${CMAKE_PREFIX_PATH}"
    # The CMake setup forces RPATH to be the Conda prefix
    export CPP_INSTALL_PREFIX="${CONDA_PREFIX}"
  else
    export CPP_INSTALL_PREFIX="${ARROW_TMPDIR}/local"
  fi

  export CMAKE_BUILD_PARALLEL_LEVEL=${CMAKE_BUILD_PARALLEL_LEVEL:-${NPROC}}
  export BUILD_ALL=1
  export ADBC_BUILD_TESTS=ON
  export ADBC_CMAKE_ARGS="-DADBC_INSTALL_NAME_RPATH=OFF"
  export ADBC_USE_ASAN=OFF
  export ADBC_USE_UBSAN=OFF
  "${ADBC_DIR}/ci/scripts/cpp_build.sh" "${ADBC_SOURCE_DIR}" "${ARROW_TMPDIR}/cpp-build" "${CPP_INSTALL_PREFIX}"
  # FlightSQL driver requires running database for testing
  export BUILD_DRIVER_FLIGHTSQL=0
  # PostgreSQL driver requires running database for testing
  export BUILD_DRIVER_POSTGRESQL=0
  # Snowflake driver requires snowflake creds for testing
  export BUILD_DRIVER_SNOWFLAKE=0
  "${ADBC_DIR}/ci/scripts/cpp_test.sh" "${ARROW_TMPDIR}/cpp-build" "${CPP_INSTALL_PREFIX}"
  export BUILD_DRIVER_FLIGHTSQL=1
  export BUILD_DRIVER_POSTGRESQL=1
  export BUILD_DRIVER_SNOWFLAKE=1
}

test_java() {
  show_header "Build and test Java libraries"

  # Build and test Java (Requires Maven >= 3.6.3)
  maybe_setup_conda maven || exit 1

  "${ADBC_DIR}/ci/scripts/java_build.sh" "${ADBC_SOURCE_DIR}" "${ARROW_TMPDIR}/java"
  "${ADBC_DIR}/ci/scripts/java_test.sh" "${ADBC_SOURCE_DIR}"
}

test_python() {
  show_header "Build and test Python libraries"

  # Build and test Python
  maybe_setup_virtualenv cython duckdb pandas polars protobuf pyarrow pytest setuptools_scm setuptools importlib_resources || exit 1
  # XXX: pin Python for now since various other packages haven't caught up
  maybe_setup_conda --file "${ADBC_DIR}/ci/conda_env_python.txt" python=3.12 || exit 1

  if [ "${USE_CONDA}" -gt 0 ]; then
    CMAKE_PREFIX_PATH="${CONDA_BACKUP_CMAKE_PREFIX_PATH}:${CMAKE_PREFIX_PATH}"
    # The CMake setup forces RPATH to be the Conda prefix
    local -r install_prefix="${CONDA_PREFIX}"
  else
    local -r install_prefix="${ARROW_TMPDIR}/local"
  fi

  "${ADBC_DIR}/ci/scripts/python_build.sh" "${ADBC_SOURCE_DIR}" "${ARROW_TMPDIR}/python-build" "${install_prefix}"
  "${ADBC_DIR}/ci/scripts/python_test.sh" "${ADBC_SOURCE_DIR}" "${ARROW_TMPDIR}/python-build" "${install_prefix}"
}

test_r() {
  show_header "Build and test R libraries"

  maybe_setup_conda --file "${ADBC_DIR}/ci/conda_env_r.txt" || exit 1

  rm -rf "${ARROW_TMPDIR}/r"
  mkdir "${ARROW_TMPDIR}/r"
  mkdir "${ARROW_TMPDIR}/r/tmplib"

  R_LIBS_USER="${ARROW_TMPDIR}/r/tmplib" R -e 'install.packages("nanoarrow", repos = "https://cloud.r-project.org/")' --vanilla
  R_LIBS_USER="${ARROW_TMPDIR}/r/tmplib" R -e 'install.packages("openssl", repos = "https://cloud.r-project.org/")' --vanilla
  R_LIBS_USER="${ARROW_TMPDIR}/r/tmplib" R -e 'if (!requireNamespace("testthat", quietly = TRUE)) install.packages("testthat", repos = "https://cloud.r-project.org/")' --vanilla
  R CMD INSTALL "${ADBC_SOURCE_DIR}/r/adbcdrivermanager" --preclean --library="${ARROW_TMPDIR}/r/tmplib"
  R CMD INSTALL "${ADBC_SOURCE_DIR}/r/adbcsqlite" --preclean --library="${ARROW_TMPDIR}/r/tmplib"
  R CMD INSTALL "${ADBC_SOURCE_DIR}/r/adbcpostgresql" --preclean --library="${ARROW_TMPDIR}/r/tmplib"
  R CMD INSTALL "${ADBC_SOURCE_DIR}/r/adbcsnowflake" --preclean --library="${ARROW_TMPDIR}/r/tmplib"

  pushd "${ARROW_TMPDIR}/r"
  R CMD build "${ADBC_SOURCE_DIR}/r/adbcdrivermanager"
  R CMD build "${ADBC_SOURCE_DIR}/r/adbcsqlite"
  R CMD build "${ADBC_SOURCE_DIR}/r/adbcpostgresql"
  R CMD build "${ADBC_SOURCE_DIR}/r/adbcsnowflake"
  local -r adbcdrivermanager_tar_gz="$(ls adbcdrivermanager_*.tar.gz)"
  local -r adbcsqlite_tar_gz="$(ls adbcsqlite_*.tar.gz)"
  local -r adbcpostgresql_tar_gz="$(ls adbcpostgresql_*.tar.gz)"
  local -r adbcsnowflake_tar_gz="$(ls adbcsnowflake_*.tar.gz)"

  R_LIBS_USER="${ARROW_TMPDIR}/r/tmplib" R CMD check "${adbcdrivermanager_tar_gz}" --no-manual
  R_LIBS_USER="${ARROW_TMPDIR}/r/tmplib" R CMD check "${adbcsqlite_tar_gz}" --no-manual
  R_LIBS_USER="${ARROW_TMPDIR}/r/tmplib" R CMD check "${adbcpostgresql_tar_gz}" --no-manual
  R_LIBS_USER="${ARROW_TMPDIR}/r/tmplib" R CMD check "${adbcsnowflake_tar_gz}" --no-manual
  popd
}

test_glib() {
  show_header "Build and test C GLib libraries"

  if [[ "${USE_CONDA}" -gt 0 && "$(uname -p)" = "arm" ]]; then
    echo "conda-forge does not have arm builds of arrow-c-glib"
    return
  fi

  # Build and test C GLib
  maybe_setup_conda --file "${ADBC_DIR}/ci/conda_env_glib.txt" || exit 1
  maybe_setup_virtualenv meson || exit 1

  # Install bundler if doesn't exist
  if ! bundle --version; then
    # This is for old Ruby. Recent Ruby bundles Bundler.
    export GEM_HOME="${ARROW_TMPDIR}/glib-build/gems"
    export PATH="${GEM_HOME}/bin:${PATH}"
    gem install --no-document bundler
  fi

  if [ "${USE_CONDA}" -gt 0 ]; then
    # The CMake setup forces RPATH to be the Conda prefix
    local -r install_prefix="${CONDA_PREFIX}"
  else
    local -r install_prefix="${ARROW_TMPDIR}/local"
  fi

  "${ADBC_DIR}/ci/scripts/glib_build.sh" "${ADBC_SOURCE_DIR}" "${ARROW_TMPDIR}/glib-build" "${install_prefix}"
  "${ADBC_DIR}/ci/scripts/glib_test.sh" "${ADBC_SOURCE_DIR}" "${ARROW_TMPDIR}/glib-build" "${install_prefix}"
}

test_csharp() {
  show_header "Build and test C# libraries"

  maybe_setup_dotnet
  maybe_setup_conda dotnet || exit 1

  export DOTNET_ROLL_FORWARD=LatestMajor

  "${ADBC_DIR}/ci/scripts/csharp_build.sh" "${ADBC_SOURCE_DIR}"
  "${ADBC_DIR}/ci/scripts/csharp_test.sh" "${ADBC_SOURCE_DIR}"
}

test_js() {
  show_header "Build and test JavaScript libraries"

  maybe_setup_nodejs || exit 1
  maybe_setup_conda nodejs=16 || exit 1

  if ! command -v yarn &> /dev/null; then
    npm install -g yarn
  fi

  echo "JS is not implemented"
}

test_go() {
  show_header "Build and test Go libraries"

  maybe_setup_go || exit 1
  # apache/arrow-adbc#517: `go build` calls git. Don't assume system
  # has git; even if it's there, go_build.sh sets DYLD_LIBRARY_PATH
  # which can interfere with system git.
  maybe_setup_conda compilers git go || exit 1

  if [ "${USE_CONDA}" -gt 0 ]; then
    # The CMake setup forces RPATH to be the Conda prefix
    local -r install_prefix="${CONDA_PREFIX}"
  else
    local -r install_prefix="${ARROW_TMPDIR}/local"
  fi

  export CGO_ENABLED=1
  "${ADBC_DIR}/ci/scripts/go_build.sh" "${ADBC_SOURCE_DIR}" "${ARROW_TMPDIR}/go-build" "${install_prefix}"
  "${ADBC_DIR}/ci/scripts/go_test.sh" "${ADBC_SOURCE_DIR}" "${ARROW_TMPDIR}/go-build" "${install_prefix}"
}

test_rust() {
  show_header "Build and test Rust libraries"

  maybe_setup_rust || exit 1
  maybe_setup_conda rust || exit 1

  # We expect the C++ libraries to exist.
  export ADBC_SNOWFLAKE_GO_LIB_DIR="${CPP_INSTALL_PREFIX}/lib"
  # XXX(https://github.com/apache/arrow-adbc/issues/3288)
  if [[ -n "${CC}" ]]; then
      export RUSTDOCFLAGS="-Clinker=${CC}"
  fi
  "${ADBC_DIR}/ci/scripts/rust_build.sh" "${ADBC_SOURCE_DIR}"
  "${ADBC_DIR}/ci/scripts/rust_test.sh" "${ADBC_SOURCE_DIR}" "${CPP_INSTALL_PREFIX}"
}

ensure_source_directory() {
  show_header "Ensuring source directory"

  dist_name="apache-arrow-adbc-${VERSION}"

  if [ "${SOURCE_KIND}" = "local" ]; then
    # Local arrow repository, testing repositories should be already present
    if [ -z "$ADBC_SOURCE_DIR" ]; then
      export ADBC_SOURCE_DIR="${ADBC_DIR}"
    fi
    echo "Verifying local Arrow checkout at ${ADBC_SOURCE_DIR}"
  elif [ "${SOURCE_KIND}" = "git" ]; then
    # Remote arrow repository, testing repositories must be cloned
    echo "Verifying Arrow repository ${SOURCE_REPOSITORY} with revision checkout ${VERSION}"
    export ADBC_SOURCE_DIR="${ARROW_TMPDIR}/arrow-adbc"
    if [ ! -d "${ADBC_SOURCE_DIR}" ]; then
      git clone --recurse-submodules https://github.com/$SOURCE_REPOSITORY $ADBC_SOURCE_DIR
      git -C $ADBC_SOURCE_DIR checkout $VERSION
    fi
  else
    # Release tarball, testing repositories must be cloned separately
    echo "Verifying official Arrow release candidate ${VERSION}-rc${RC_NUMBER}"
    export ADBC_SOURCE_DIR="${ARROW_TMPDIR}/${dist_name}"
    if [ ! -d "${ADBC_SOURCE_DIR}" ]; then
      pushd $ARROW_TMPDIR
      mkdir -p "${ADBC_SOURCE_DIR}"
      fetch_archive ${dist_name}
      tar -C "${ADBC_SOURCE_DIR}" --strip-components 1 -xf "${dist_name}.tar.gz"
      popd
    fi
  fi

  echo "Fetching Arrow repository ${ARROW_REPOSITORY}"
  export ARROW_SOURCE_DIR="${ARROW_TMPDIR}/arrow"
  if [ ! -d "${ARROW_SOURCE_DIR}" ]; then
    git clone --depth=1 https://github.com/$ARROW_REPOSITORY $ARROW_SOURCE_DIR
  fi

  source "${ADBC_SOURCE_DIR}/dev/release/versions.env"
  echo "Versions:"
  echo "Release: ${RELEASE} (requested: ${VERSION})"
  echo "C#: ${VERSION_CSHARP}"
  echo "Java: ${VERSION_JAVA}"
  echo "C/C++/GLib/Go/Python/Ruby: ${VERSION_NATIVE}"
  echo "R: ${VERSION_R}"
  echo "Rust: ${VERSION_RUST}"
}

test_source_distribution() {
  export ARROW_HOME=$ARROW_TMPDIR/install
  export CMAKE_PREFIX_PATH=$ARROW_HOME${CMAKE_PREFIX_PATH:+:${CMAKE_PREFIX_PATH}}
  export PARQUET_HOME=$ARROW_TMPDIR/install
  export PKG_CONFIG_PATH=$ARROW_HOME/lib/pkgconfig${PKG_CONFIG_PATH:+:${PKG_CONFIG_PATH}}

  if [ "$(uname)" == "Darwin" ]; then
    NPROC=$(sysctl -n hw.ncpu)
    export DYLD_LIBRARY_PATH=$ARROW_HOME/lib:${DYLD_LIBRARY_PATH:-}
  else
    NPROC=$(nproc)
    export LD_LIBRARY_PATH=$ARROW_HOME/lib:${LD_LIBRARY_PATH:-}
  fi

  pushd $ADBC_SOURCE_DIR

  if [ ${TEST_CPP} -gt 0 ]; then
    test_cpp
  fi
  if [ ${TEST_CSHARP} -gt 0 ]; then
    test_csharp
  fi
  if [ ${TEST_GLIB} -gt 0 ]; then
    test_glib
  fi
  if [ ${TEST_GO} -gt 0 ]; then
    test_go
  fi
  if [ ${TEST_JAVA} -gt 0 ]; then
    test_java
  fi
  if [ ${TEST_PYTHON} -gt 0 ]; then
    test_python
  fi
  if [ ${TEST_R} -gt 0 ]; then
    test_r
  fi
  if [ ${TEST_RUST} -gt 0 ]; then
    test_rust
  fi

  popd
}

test_binary_distribution() {
  if [ $((${TEST_BINARY} + ${TEST_JARS} + ${TEST_WHEELS})) -gt 0 ]; then
    show_header "Downloading binary artifacts"
    export BINARY_DIR="${ARROW_TMPDIR}/binaries"
    mkdir -p "${BINARY_DIR}"

    ${PYTHON:-python3} "$ARROW_SOURCE_DIR/dev/release/download_rc_binaries.py" \
                       $VERSION $RC_NUMBER \
                       --dest="${BINARY_DIR}" \
                       --num_parallel 4 \
                       --package_type=github \
                       --repository="${SOURCE_REPOSITORY}" \
                       --tag="apache-arrow-adbc-${VERSION}-rc${RC_NUMBER}"
  fi

  if [ ${TEST_BINARY} -gt 0 ]; then
    test_binary
  fi
  if [ ${TEST_APT} -gt 0 ]; then
    test_apt
  fi
  if [ ${TEST_YUM} -gt 0 ]; then
    test_yum
  fi
  if [ ${TEST_WHEELS} -gt 0 ]; then
    test_wheels
  fi
  if [ ${TEST_JARS} -gt 0 ]; then
    test_jars
  fi
}

test_linux_wheels() {
  if [ "$(uname -m)" = "aarch64" ]; then
    local arch="aarch64"
  else
    local arch="x86_64"
  fi

  local python_versions="${TEST_PYTHON_VERSIONS:-3.10 3.11 3.12 3.13 3.14}"

  for python in ${python_versions}; do
    local pyver=${python/m}
    show_header "Testing Python ${pyver} wheel for platform manylinux"
    CONDA_ENV=wheel-${pyver}-${arch} PYTHON_VERSION=${pyver} maybe_setup_conda || exit 1
    VENV_ENV=wheel-${pyver}-${arch} PYTHON_VERSION=${pyver} maybe_setup_virtualenv || continue
    pip install --force-reinstall \
        adbc_*-${VERSION_NATIVE}-cp${pyver/.}-cp${python/.}-manylinux*${arch}*.whl \
        adbc_*-${VERSION_NATIVE}-py3-none-manylinux*${arch}*.whl
    ${ADBC_DIR}/ci/scripts/python_wheel_unix_test.sh ${ADBC_SOURCE_DIR}
  done
}

test_macos_wheels() {
  # apple silicon processor
  if [ "$(uname -m)" = "arm64" ]; then
    local platform_tags="arm64"
  else
    local platform_tags="x86_64"
  fi

  local python_versions="${TEST_PYTHON_VERSIONS:-3.10 3.11 3.12 3.13 3.14}"

  # verify arch-native wheels inside an arch-native conda environment
  for python in ${python_versions}; do
    local pyver=${python/m}
    for platform in ${platform_tags}; do
      show_header "Testing Python ${pyver} wheel for platform ${platform}"

      CONDA_ENV=wheel-${pyver}-${platform} PYTHON_VERSION=${pyver} maybe_setup_conda || exit 1
      VENV_ENV=wheel-${pyver}-${platform} PYTHON_VERSION=${pyver} maybe_setup_virtualenv || continue

      pip install --force-reinstall \
          adbc_*-${VERSION_NATIVE}-cp${pyver/.}-cp${python/.}-macosx_*_${platform}.whl \
          adbc_*-${VERSION_NATIVE}-py3-none-macosx_*_${platform}.whl
      ${ADBC_DIR}/ci/scripts/python_wheel_unix_test.sh ${ADBC_SOURCE_DIR}
    done
  done
}

test_wheels() {
  show_header "Test Python wheels"
  maybe_setup_conda python || exit 1

  local wheels_dir=
  if [ "${SOURCE_KIND}" = "local" ]; then
    echo "Binary verification of local wheels is not currently implemented"
    exit 1
  else
    wheels_dir=${BINARY_DIR}
  fi

  pushd ${wheels_dir}

  if [ "$(uname)" == "Darwin" ]; then
    test_macos_wheels
  else
    test_linux_wheels
  fi

  popd
}

test_jars() {
  show_header "Testing Java jars"
  maybe_setup_conda maven python || exit 1

  # TODO: actually verify the JARs
  local -r packages=(adbc-core adbc-driver-flight-sql adbc-driver-jdbc adbc-driver-manager)
  local -r components=(".jar" "-javadoc.jar" "-sources.jar")
  for package in "${packages[@]}"; do
      for component in "${components[@]}"; do
          local filename="${BINARY_DIR}/${package}-${VERSION_JAVA}${component}"
          if [[ ! -f "${filename}" ]];  then
             echo "ERROR: missing artifact ${filename}"
             return 1
          else
             echo "Found artifact ${filename}"
          fi
      done
  done
}

# By default test all functionalities.
# To deactivate one test, deactivate the test and all of its dependents
# To explicitly select one test, set TEST_DEFAULT=0 TEST_X=1
: ${TEST_DEFAULT:=1}

# Verification groups
: ${TEST_SOURCE:=${TEST_DEFAULT}}
: ${TEST_BINARIES:=${TEST_DEFAULT}}

# Binary verification tasks
: ${TEST_APT:=${TEST_BINARIES}}
: ${TEST_BINARY:=${TEST_BINARIES}}
: ${TEST_JARS:=${TEST_BINARIES}}
: ${TEST_WHEELS:=${TEST_BINARIES}}
: ${TEST_YUM:=${TEST_BINARIES}}

# Source verification tasks
: ${TEST_JAVA:=${TEST_SOURCE}}
: ${TEST_CPP:=${TEST_SOURCE}}
: ${TEST_CSHARP:=${TEST_SOURCE}}
: ${TEST_GLIB:=${TEST_SOURCE}}
: ${TEST_PYTHON:=${TEST_SOURCE}}
: ${TEST_JS:=${TEST_SOURCE}}
: ${TEST_GO:=${TEST_SOURCE}}
: ${TEST_R:=${TEST_SOURCE}}
: ${TEST_RUST:=${TEST_SOURCE}}

# Automatically test if its activated by a dependent
TEST_CPP=$((${TEST_CPP} + ${TEST_GO} + ${TEST_GLIB} + ${TEST_PYTHON} + ${TEST_RUST}))

# Execute tests in a conda environment
: ${USE_CONDA:=0}

TEST_SUCCESS=no

setup_tempdir
ensure_source_directory
test_source_distribution
test_binary_distribution

TEST_SUCCESS=yes

echo 'Release candidate looks good!'
exit 0
