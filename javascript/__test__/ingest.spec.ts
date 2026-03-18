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
import { AdbcDatabase, AdbcConnection, IngestMode } from '../lib/index.js'
import { tableFromArrays, Table } from 'apache-arrow'

let db: AdbcDatabase
let conn: AdbcConnection

before(async () => {
  db = await createSqliteDatabase()
  conn = await db.connect()
})

after(async () => {
  try {
    await conn?.close()
    await db?.close()
  } catch {
    // ignore
  }
})

test('ingest: create mode inserts data into a new table', async () => {
  const data = tableFromArrays({ id: [1, 2, 3], name: ['alice', 'bob', 'carol'] })
  const rowCount = await conn.ingest('ingest_create', data)
  assert.strictEqual(rowCount, 3)

  const result = await conn.query('SELECT id, name FROM ingest_create ORDER BY id')
  assert.strictEqual(result.numRows, 3)
  assert.strictEqual(result.getChildAt(0)?.get(0), 1)
  assert.strictEqual(result.getChildAt(1)?.get(0), 'alice')
})

test('ingest: create mode fails if table already exists', async () => {
  const data = tableFromArrays({ id: [1] })
  await conn.ingest('ingest_create_dup', data)
  await assert.rejects(() => conn.ingest('ingest_create_dup', data))
})

test('ingest: append mode adds rows to an existing table', async () => {
  const initial = tableFromArrays({ id: [1], name: ['alice'] })
  await conn.ingest('ingest_append', initial)

  const more = tableFromArrays({ id: [2], name: ['bob'] })
  const rowCount = await conn.ingest('ingest_append', more, { mode: IngestMode.Append })
  assert.strictEqual(rowCount, 1)

  const result = await conn.query('SELECT id FROM ingest_append ORDER BY id')
  assert.strictEqual(result.numRows, 2)
})

test('ingest: replace mode drops and recreates the table', async () => {
  const initial = tableFromArrays({ id: [1, 2, 3] })
  await conn.ingest('ingest_replace', initial)

  const replacement = tableFromArrays({ id: [99] })
  await conn.ingest('ingest_replace', replacement, { mode: IngestMode.Replace })

  const result = await conn.query('SELECT id FROM ingest_replace')
  assert.strictEqual(result.numRows, 1)
  assert.strictEqual(result.getChildAt(0)?.get(0), 99)
})

test('ingest: multi-batch table inserts all batches', async () => {
  const batch = tableFromArrays({ id: [1], name: ['alice'] }).batches[0]
  const data = new Table([batch, batch])
  assert.strictEqual(data.batches.length, 2)

  const rowCount = await conn.ingest('ingest_multi_batch', data)
  assert.strictEqual(rowCount, 2)

  const result = await conn.query('SELECT id FROM ingest_multi_batch')
  assert.strictEqual(result.numRows, 2)
})

test('ingest: create_append mode creates table if not exists then appends', async () => {
  const data = tableFromArrays({ id: [1] })
  await conn.ingest('ingest_create_append', data, { mode: IngestMode.CreateAppend })
  await conn.ingest('ingest_create_append', data, { mode: IngestMode.CreateAppend })

  const result = await conn.query('SELECT id FROM ingest_create_append')
  assert.strictEqual(result.numRows, 2)
})
