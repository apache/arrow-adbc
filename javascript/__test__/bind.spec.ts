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
import { createSqliteDatabase, createTestTable } from './test_utils'
import { AdbcDatabase, AdbcConnection, AdbcStatement } from '../lib/index.js'
import { tableFromArrays, Table } from 'apache-arrow'

let db: AdbcDatabase
let conn: AdbcConnection
let stmt: AdbcStatement

before(async () => {
  db = await createSqliteDatabase()
  conn = await db.connect()
  stmt = await conn.createStatement()
  await createTestTable(stmt, 'bind_test')
})

after(async () => {
  try {
    await stmt?.close()
    await conn?.close()
    await db?.close()
  } catch {
    // ignore
  }
})

test('statement: bind and query data', async () => {
  const recordBatchToBind = tableFromArrays({
    id: [null],
    name: ['test_name'],
  })

  assert.strictEqual(recordBatchToBind.numRows, 1)
  await stmt.bind(recordBatchToBind)

  await stmt.setSqlQuery('INSERT INTO bind_test (id, name) VALUES (?, ?)')
  const insertResult = await stmt.executeUpdate()
  assert.strictEqual(insertResult, 1)

  await stmt.setSqlQuery('SELECT id, name FROM bind_test')
  const reader = await stmt.executeQuery()

  let rowCount = 0
  for await (const batch of reader) {
    rowCount += batch.numRows
    const idVector = batch.getChild('id')
    const nameVector = batch.getChild('name')
    assert.strictEqual(idVector?.get(0), null)
    assert.strictEqual(nameVector?.get(0), 'test_name')
  }

  assert.strictEqual(rowCount, 1)
})

test('statement: bind multi-batch table throws descriptive error', async () => {
  // Both batches must share the same schema instance for Table to accept them
  const base = tableFromArrays({ id: [10], name: ['first'] })
  const batch1 = base.batches[0]
  const batch2 = base.batches[0] // same schema, reused to construct a multi-batch Table
  const multiTable = new Table([batch1, batch2])
  assert.strictEqual(multiTable.batches.length, 2)

  const stmt2 = await conn.createStatement()
  const error = await stmt2.bind(multiTable).catch((e) => e)
  assert.ok(error instanceof Error)
  assert.match(error.message, /bind\(\).*batches|batches.*bind\(\)/i)
  await stmt2.close()
})
