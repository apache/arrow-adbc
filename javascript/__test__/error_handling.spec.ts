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
import { AdbcDatabase, AdbcConnection, AdbcStatement, AdbcError } from '../lib/index.js'

let db: AdbcDatabase
let conn: AdbcConnection
let stmt: AdbcStatement

before(async () => {
  db = await createSqliteDatabase()
  conn = await db.connect()
  stmt = await conn.createStatement()
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

test('error: invalid sql syntax', async () => {
  let error: unknown
  try {
    await stmt.setSqlQuery('SELECT * FROM') // Syntax error
    const reader = await stmt.executeQuery()
    for await (const _ of reader) {
    }
  } catch (e) {
    error = e
  }

  assert.ok(error instanceof AdbcError)
  assert.match(error.message, /syntax error|incomplete input/i)
  assert.strictEqual(error.code, 'InvalidArguments')
  // SQLite does not expose a numeric vendor code; the ADBC sentinel (INT32_MIN) is filtered to undefined
  assert.strictEqual(error.vendorCode, undefined)
  // SQLite does not set a SQLSTATE for this error
  assert.strictEqual(error.sqlState, undefined)
})

test('error: table not found', async () => {
  let error: unknown
  try {
    await stmt.setSqlQuery('SELECT * FROM non_existent_table')
    const reader = await stmt.executeQuery()
    for await (const _ of reader) {
    }
  } catch (e) {
    error = e
  }

  assert.ok(error instanceof AdbcError)
  assert.match(error.message, /no such table/i)
  assert.strictEqual(error.code, 'InvalidArguments')
  assert.strictEqual(error.vendorCode, undefined)
  assert.strictEqual(error.sqlState, undefined)
})

test('error: constraint violation', async () => {
  const setupStmt = await conn.createStatement()
  await setupStmt.setSqlQuery('CREATE TABLE IF NOT EXISTS err_test (id INTEGER PRIMARY KEY)')
  await setupStmt.executeUpdate()
  await setupStmt.setSqlQuery('INSERT INTO err_test (id) VALUES (1)')
  await setupStmt.executeUpdate()
  await setupStmt.close()

  await stmt.setSqlQuery('INSERT INTO err_test (id) VALUES (1)')
  let error: unknown
  try {
    await stmt.executeUpdate()
  } catch (e) {
    error = e
  }

  assert.ok(error instanceof AdbcError)
  assert.match(error.code, /AlreadyExists|Integrity|IO/)
  assert.strictEqual(error.vendorCode, undefined)
  assert.strictEqual(error.sqlState, undefined)
})

test('error: unsupported option', () => {
  // SQLite does not support the read_only connection option at runtime
  let error: unknown
  try {
    conn.setOption('adbc.connection.read_only', 'true')
  } catch (e) {
    error = e
  }

  assert.ok(error instanceof AdbcError)
  assert.strictEqual(error.code, 'NotImplemented')
  assert.strictEqual(error.vendorCode, undefined)
  assert.strictEqual(error.sqlState, undefined)
})
