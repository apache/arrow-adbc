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

set -euo pipefail

# Pin the Kotoba compiler used by the kotoba/ binding.
KOTOBA_VERSION="${KOTOBA_VERSION:-0.7.2}"
KOTOBA_PREFIX="${1:-${HOME}/.local/kotoba-${KOTOBA_VERSION}}"
KOTOBA_ARCHIVE="kotoba-linux-amd64.tar.gz"
KOTOBA_URL="https://github.com/kotoba-lang/kotoba/releases/download/v${KOTOBA_VERSION}/${KOTOBA_ARCHIVE}"
KOTOBA_SHA256="95e225461e1b8a21849b251e8c8b654693d2c8a516b258532771651e978e1977"

if [[ -x "${KOTOBA_PREFIX}/kotoba" ]]; then
  echo "Using existing ${KOTOBA_PREFIX}/kotoba"
else
  tmpdir="$(mktemp -d)"
  trap 'rm -rf "${tmpdir}"' EXIT
  curl -fsSL -o "${tmpdir}/${KOTOBA_ARCHIVE}" "${KOTOBA_URL}"
  echo "${KOTOBA_SHA256}  ${tmpdir}/${KOTOBA_ARCHIVE}" | sha256sum -c -
  mkdir -p "${KOTOBA_PREFIX}"
  tar -xzf "${tmpdir}/${KOTOBA_ARCHIVE}" -C "${KOTOBA_PREFIX}"
fi

if [[ -n "${GITHUB_PATH:-}" ]]; then
  echo "${KOTOBA_PREFIX}" >> "${GITHUB_PATH}"
fi
export PATH="${KOTOBA_PREFIX}:${PATH}"
command -v kotoba
