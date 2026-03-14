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
import { createSqliteDatabase, createTestTable, dumpReader } from './test_utils'
import { AdbcDatabase, AdbcConnection, AdbcStatement } from '../lib/index.js'
import { Table, tableFromArrays } from 'apache-arrow'

let db: AdbcDatabase
let conn: AdbcConnection
let stmt: AdbcStatement

before(async () => {
  db = await createSqliteDatabase()
  conn = await db.connect()
  stmt = await conn.createStatement()

  await createTestTable(stmt, 'query_test')
  await stmt.setSqlQuery(`INSERT INTO query_test (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')`)
  await stmt.executeUpdate()
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

test('query: SELECT returns correct rows', async () => {
  await stmt.setSqlQuery('SELECT id, name FROM query_test ORDER BY id')
  const reader = await stmt.executeQuery()

  const rows: { id: unknown; name: unknown }[] = []
  for await (const batch of reader) {
    const idCol = batch.getChild('id')
    const nameCol = batch.getChild('name')
    for (let i = 0; i < batch.numRows; i++) {
      rows.push({ id: idCol?.get(i), name: nameCol?.get(i) })
    }
  }

  assert.strictEqual(rows.length, 3)
  assert.deepStrictEqual(rows[0], { id: 1n, name: 'alice' })
  assert.deepStrictEqual(rows[1], { id: 2n, name: 'bob' })
  assert.deepStrictEqual(rows[2], { id: 3n, name: 'carol' })
})

test('query: executeUpdate returns affected row count', async () => {
  const stmt = await conn.createStatement()
  try {
    await stmt.setSqlQuery(`UPDATE query_test SET name = 'updated' WHERE id = 1`)
    const affected = await stmt.executeUpdate()
    assert.strictEqual(typeof affected, 'number')
    assert.strictEqual(affected, 1)

    // Verify the change was applied
    const table = await conn.query('SELECT id, name FROM query_test WHERE id = 1')
    assert.strictEqual(table.numRows, 1)
    assert.strictEqual(table.getChild('id')?.get(0), 1n)
    assert.strictEqual(table.getChild('name')?.get(0), 'updated')
  } finally {
    await stmt.close()
  }
})

test('query: conn.query() returns an Arrow Table', async () => {
  // id=2 (bob) is never mutated by other tests in this file
  const table = await conn.query('SELECT id, name FROM query_test WHERE id = 2')
  assert.ok(table instanceof Table)
  assert.strictEqual(table.numCols, 2)
  assert.strictEqual(table.numRows, 1)
  assert.strictEqual(table.getChild('id')?.get(0), 2n)
  assert.strictEqual(table.getChild('name')?.get(0), 'bob')
})

test('query: conn.query() with bound params', async () => {
  // id=2 (bob) is never mutated by other tests in this file
  const params = tableFromArrays({ id: [2] })
  const table = await conn.query('SELECT id, name FROM query_test WHERE id = ?', params)
  assert.ok(table instanceof Table)
  assert.strictEqual(table.numRows, 1)
  assert.strictEqual(table.getChild('id')?.get(0), 2n)
  assert.strictEqual(table.getChild('name')?.get(0), 'bob')
})

test('query: conn.queryStream() returns a RecordBatchReader', async () => {
  // id=2 (bob) is never mutated by other tests in this file
  const reader = await conn.queryStream('SELECT id, name FROM query_test WHERE id = 2')
  const rows = await dumpReader(reader)
  assert.strictEqual(rows.length, 1)
  assert.strictEqual(rows[0].id, 2n)
  assert.strictEqual(rows[0].name, 'bob')
})

test('query: conn.execute() returns affected row count', async () => {
  const affected = await conn.execute(`UPDATE query_test SET name = 'via_execute' WHERE id = 3`)
  assert.strictEqual(affected, 1)

  // Verify the change was applied
  const table = await conn.query('SELECT id, name FROM query_test WHERE id = 3')
  assert.strictEqual(table.numRows, 1)
  assert.strictEqual(table.getChild('id')?.get(0), 3n)
  assert.strictEqual(table.getChild('name')?.get(0), 'via_execute')
})

test('query: conn.execute() with bound params inserts a row', async () => {
  const params = tableFromArrays({ id: [99], name: ['bound_insert'] })
  const affected = await conn.execute('INSERT INTO query_test (id, name) VALUES (?, ?)', params)
  assert.strictEqual(affected, 1)

  const table = await conn.query('SELECT id, name FROM query_test WHERE id = 99')
  assert.strictEqual(table.numRows, 1)
  assert.strictEqual(table.getChild('id')?.get(0), 99n)
  assert.strictEqual(table.getChild('name')?.get(0), 'bound_insert')
})

test('query: empty result set', async () => {
  // Use a fresh statement — ADBC statements should not be reused after executeQuery
  const stmt = await conn.createStatement()
  try {
    await stmt.setSqlQuery('SELECT * FROM query_test WHERE id = 9999')
    const reader = await stmt.executeQuery()

    let rowCount = 0
    for await (const batch of reader) {
      rowCount += batch.numRows
    }

    assert.strictEqual(rowCount, 0)
  } finally {
    await stmt.close()
  }
})
