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

# Apache Arrow ADBC: Node.js Driver Manager

Node.js bindings for the [Arrow Database Connectivity (ADBC)](https://arrow.apache.org/adbc/) standard.
Built on a native [NAPI](https://nodejs.org/api/n-api.html) addon — requires Node.js 22+ and does not
support browser or Deno environments. Bun is not officially tested.

**Alpha: APIs may change without notice.**

## Installation

```bash
npm install adbc-driver-manager apache-arrow
```

## Usage

The `driver` option accepts either a full path to a shared library or a short
name. When using a short name, the driver manager searches system and user
paths for a matching ADBC driver manifest or library.

```typescript
import { AdbcDatabase } from 'adbc-driver-manager'

// Full path to a driver shared library
const db = new AdbcDatabase({ driver: '/path/to/libadbc_driver_sqlite.dylib' })
```

```typescript
// Short name (resolves from system/user paths)
const db = new AdbcDatabase({ driver: 'sqlite' })
```

Once you have a database, open a connection and run queries:

```typescript
const connection = await db.connect()

// Execute a query and iterate Arrow RecordBatches
const reader = await connection.query('SELECT 1 AS value')
for await (const batch of reader) {
  console.log(batch.toArray())
}

// Or use the lower-level statement API
const stmt = await connection.createStatement()
await stmt.setSqlQuery('SELECT 1 AS value')
const result = await stmt.executeQuery()
for await (const batch of result) {
  console.log(`Received batch with ${batch.numRows} rows`)
}
await stmt.close()

await connection.close()
await db.close()
```

## Development

### Prerequisites

- Node.js 22+
- Rust (latest stable)
- CMake 3.14+ and a C/C++ compiler (for building the driver libraries)
- npm (usually comes with Node.js)

### Building from Source

1. Install dependencies:

   ```bash
   npm install
   ```

2. Build the Rust addon:

   ```bash
   npm run build:debug   # debug build (faster)
   npm run build         # release build
   ```

### Testing

The tests require a built ADBC driver library (e.g. SQLite). Build everything including the SQLite driver with:

```bash
npm run build:driver
```

This runs CMake to compile the C ADBC drivers into `build/lib/` and builds the Rust Node.js addon. Once built, run the tests:

```bash
npm test
```

## License

Apache-2.0
