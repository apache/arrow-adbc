// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import { test, before, after } from 'node:test'
import assert from 'node:assert/strict'
import { createSqliteDatabase } from './test_utils'
import { AdbcDatabase, AdbcConnection, AdbcStatement } from '../lib/index.js'

let db: AdbcDatabase | undefined
let conn: AdbcConnection | undefined
let stmt: AdbcStatement | undefined

before(async () => {
  db = await createSqliteDatabase()
  conn = await db.connect()
  stmt = await conn.createStatement()
  await stmt.setSqlQuery('SELECT 1 as val')
})

after(async () => {
  try {
    if (stmt) await stmt.close()
  } catch {}
  try {
    if (conn) await conn.close()
  } catch {}
  try {
    if (db) await db.close()
  } catch {}
})

test('safety: iterator survives connection close', async () => {
  // Rationale:
  // The ADBC C API Specification states regarding Resource Management:
  // "Releasing a parent object does not automatically release child objects, but it may invalidate them."
  // (https://arrow.apache.org/adbc/current/c/api/index.html#resource-management)
  //
  // In a raw C application, accessing a Child (Iterator) after closing the Parent (Connection)
  // would often result in a Use-After-Free (Segfault).
  //
  // However, in Node.js, a Segfault is unacceptable. This driver implementation (via the Rust adbc_driver_manager)
  // uses Reference Counting (Arc) to manage the lifecycle of the underlying C structs.
  // When we "Close" the connection in JavaScript, we are releasing the JavaScript handle's
  // reference to the resource.
  //
  // The Iterator (Reader) holds its own strong reference to the underlying Statement/Connection
  // to ensure it can safely finish reading. The actual C-level teardown of the Connection
  // only occurs when *all* references (Connection handle + all active Iterators) are dropped.
  //
  // This test verifies that "Safety" mechanism: ensuring that premature closure of the parent
  // handle does not crash the process or interrupt the child stream.

  // Get Reader (which holds iterator)
  const reader = await stmt!.executeQuery()

  // Close Statement and Connection immediately.
  // The Iterator (inside Reader) should keep the underlying resources alive.
  await stmt!.close()
  stmt = undefined

  await conn!.close()
  conn = undefined

  await db!.close()
  db = undefined

  // Iterate — must still work despite all handles being closed
  let rowCount = 0
  for await (const batch of reader) {
    rowCount += batch.numRows
    const valVector = batch.getChild('val')
    assert.strictEqual(valVector?.get(0), 1n)
  }

  assert.strictEqual(rowCount, 1)
})
