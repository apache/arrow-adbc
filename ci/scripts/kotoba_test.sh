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

source_dir="${1:-$(pwd)}"
kotoba_dir="${source_dir}/kotoba"
out_dir="${2:-${source_dir}/build/kotoba}"

if ! command -v kotoba >/dev/null 2>&1; then
  echo "kotoba is required (v0.7.2)" >&2
  exit 1
fi

if ! command -v node >/dev/null 2>&1; then
  echo "node is required to run Kotoba i64-v1 wasm fixtures" >&2
  exit 1
fi

mkdir -p "${out_dir}"

expect_wasm() {
  python3 -c '
import json, pathlib, sys
result = json.load(sys.stdin)
ok = result.get("kotoba.cli/ok?")
code = result.get("kotoba.cli/code")
if not ok:
    print(result, file=sys.stderr)
    raise SystemExit(f"compile failed: {code}")
profile = result.get("kotoba.cli/data", {}).get("value-profile")
if profile != "i64-v1":
    raise SystemExit(f"expected i64-v1, got {profile}")
path = pathlib.Path(sys.argv[1])
data = path.read_bytes()
if data[:4] != b"\x00asm":
    raise SystemExit(f"{path} is not a wasm module")
print(f"emitted {code} profile={profile} bytes={len(data)}")
' "$1"
}

compile_entry() {
  local entry="$1"
  local output="$2"
  echo "compile ${entry} -> ${output}"
  kotoba compile "${entry}" \
    --source-path "${kotoba_dir}" \
    --unpinned \
    --target wasm \
    -o "${output}" \
    --json | expect_wasm "${output}"
}

compile_project() {
  local output="$1"
  echo "compile project ${kotoba_dir}/kotoba-project.edn -> ${output}"
  kotoba compile \
    --project "${kotoba_dir}/kotoba-project.edn" \
    --target wasm \
    -o "${output}" \
    --json | expect_wasm "${output}"
}

run_fixture() {
  local wasm="$1"
  node "${source_dir}/kotoba/run_fixture.mjs" "${wasm}"
}

compile_entry "${kotoba_dir}/adbc/status.kotoba" "${out_dir}/status.wasm"
compile_entry "${kotoba_dir}/adbc/error.kotoba" "${out_dir}/error.wasm"
compile_entry "${kotoba_dir}/adbc/connection.kotoba" "${out_dir}/connection.wasm"

compile_entry "${kotoba_dir}/fixtures/status.kotoba" "${out_dir}/fixture-status.wasm"
compile_entry "${kotoba_dir}/fixtures/error.kotoba" "${out_dir}/fixture-error.wasm"
compile_entry "${kotoba_dir}/fixtures/mock_connection.kotoba" "${out_dir}/fixture-mock-connection.wasm"

compile_project "${out_dir}/project.wasm"

run_fixture "${out_dir}/fixture-status.wasm"
run_fixture "${out_dir}/fixture-error.wasm"
run_fixture "${out_dir}/fixture-mock-connection.wasm"
run_fixture "${out_dir}/project.wasm"

echo "kotoba fixtures passed"
