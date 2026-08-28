<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Arrow Database Connectivity for Kotoba

In-language ADBC status and error types for [Kotoba](https://kotoba-lang.org),
compiled to WebAssembly with Kotoba 0.7.2.

Kotoba guests cannot FFI `libadbc`. This v1 binding is a small guest-side
status/error encoding plus a mock connection fixture. There is no real
database. The host owns drivers and maps a URI to an integer kind before
calling into the wasm module.

## Layout

- `adbc/status.kotoba` — `AdbcStatusCode` values from `adbc.h`
- `adbc/error.kotoba` — packed status + vendor code
- `adbc/connection.kotoba` — mock connect/close/query
- `fixtures/` — executable wasm fixtures (`main` returns `0` on success)

## Requirements

- [Kotoba](https://github.com/kotoba-lang/kotoba) 0.7.2
- Node.js 18+ (to instantiate the i64-v1 wasm fixtures)

```shell
# From the repository root
./ci/scripts/kotoba_test.sh "$(pwd)"
```

Or compile a single entry point:

```shell
kotoba compile kotoba/adbc/status.kotoba --target wasm -o status.wasm
kotoba compile kotoba/fixtures/mock_connection.kotoba \
  --source-path kotoba --unpinned --target wasm -o mock.wasm
```
