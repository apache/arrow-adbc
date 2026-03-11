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

test('options: connection setOption', () => {
  // Test setting autocommit via canonical ADBC key (supported by SQLite)
  assert.doesNotThrow(() => {
    conn.setOption('adbc.connection.autocommit', 'true')
  })

  // Test setting read_only via canonical ADBC key (not supported by SQLite at runtime)
  let errorReadOnly: unknown
  try {
    conn.setOption('adbc.connection.read_only', 'false')
  } catch (e) {
    errorReadOnly = e
  }
  assert.ok(errorReadOnly instanceof AdbcError)
  assert.strictEqual(errorReadOnly.code, 'NotImplemented')
  assert.match(errorReadOnly.message, /Unknown connection option/i)

  // Test setting generic option (SQLite driver is strict and rejects unknown options)
  let errorCustom: unknown
  try {
    conn.setOption('custom_option', 'custom_value')
  } catch (e) {
    errorCustom = e
  }
  assert.ok(errorCustom instanceof AdbcError)
  assert.strictEqual(errorCustom.code, 'NotImplemented')
})

test('options: statement setOption', () => {
  // Test setting an unknown statement option (SQLite driver is strict)
  let error: unknown
  try {
    stmt.setOption('adbc.stmt.some_option', 'value')
  } catch (e) {
    error = e
  }
  assert.ok(error instanceof AdbcError)
  assert.strictEqual(error.code, 'NotImplemented')
  assert.match(error.message, /Unknown statement option/i)
})
