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

let db: AdbcDatabase
let conn: AdbcConnection
let stmt: AdbcStatement

before(async () => {
  db = await createSqliteDatabase()
  conn = await db.connect()
  stmt = await conn.createStatement()
  await createTestTable(stmt, 'tx_test')
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

test('transaction: rollback reverts changes', async () => {
  conn.setAutoCommit(false)

  const newStmt = await conn.createStatement()
  try {
    await newStmt.setSqlQuery('INSERT INTO tx_test (id) VALUES (1)')
    await newStmt.executeUpdate()

    await conn.rollback()

    await newStmt.setSqlQuery('SELECT * FROM tx_test')
    const reader = await newStmt.executeQuery()
    let count = 0
    for await (const batch of reader) count += batch.numRows
    assert.strictEqual(count, 0)
  } finally {
    await newStmt.close()
  }
})

test('transaction: commit persists changes', async () => {
  conn.setAutoCommit(false)

  const newStmt = await conn.createStatement()
  try {
    await newStmt.setSqlQuery('INSERT INTO tx_test (id) VALUES (2)')
    const affectedRows = await newStmt.executeUpdate()
    assert.strictEqual(affectedRows, 1)

    // Verify row is visible before commit (within the transaction)
    await newStmt.setSqlQuery('SELECT * FROM tx_test')
    const readerBeforeCommit = await newStmt.executeQuery()
    let countBeforeCommit = 0
    for await (const batch of readerBeforeCommit) countBeforeCommit += batch.numRows
    assert.strictEqual(countBeforeCommit, 1)

    await conn.commit()

    // ADBC Spec: "Calling commit or rollback on the connection may invalidate active statements."
    // Create a fresh statement to read back the committed data.
    const verifyStmt = await conn.createStatement()
    try {
      await verifyStmt.setSqlQuery('SELECT * FROM tx_test')
      const reader = await verifyStmt.executeQuery()
      let count = 0
      for await (const batch of reader) count += batch.numRows
      assert.strictEqual(count, 1)
    } finally {
      await verifyStmt.close()
    }
  } finally {
    await newStmt.close()
  }
})
