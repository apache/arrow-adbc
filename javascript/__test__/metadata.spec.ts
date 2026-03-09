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

let db: AdbcDatabase
let conn: AdbcConnection
let stmt: AdbcStatement

before(async () => {
  db = await createSqliteDatabase()
  conn = await db.connect()
  stmt = await conn.createStatement()
  await createTestTable(stmt, 'metadata_test')
})

after(async () => {
  try {
    await stmt?.close()
    await conn?.close()
    await db?.close()
  } catch (e) {
    console.error('Error cleaning up test resources:', e)
  }
})

test('metadata: getTableTypes', async () => {
  const tableTypes = await dumpReader(await conn.getTableTypes())

  // Sort actual results for consistent comparison
  tableTypes.sort((a, b) => (a.table_type || '').localeCompare(b.table_type || ''))

  assert.strictEqual(tableTypes[0].table_type, 'table')
  assert.strictEqual(tableTypes[1].table_type, 'view')
})

test('metadata: getTableSchema', async () => {
  const schema = await conn.getTableSchema({ tableName: 'metadata_test' })

  assert.strictEqual(schema.fields[0].name, 'id')
  assert.strictEqual(schema.fields[0].nullable, true)
  assert.strictEqual(schema.fields[1].name, 'name')
  assert.strictEqual(schema.fields[1].nullable, true)
})

test('metadata: getObjects', async () => {
  // SQLite structure: Catalog (null/main) -> Schemas (null/main) -> Tables
  const objects = await dumpReader(
    await conn.getObjects({
      depth: 3,
      tableName: 'metadata_test',
      tableType: ['table', 'view'],
    }),
  )

  const tables = objects[0].catalog_db_schemas[0].db_schema_tables
  assert.ok(tables.some((t: { table_name: string }) => t.table_name === 'metadata_test'))
})

test('metadata: getInfo', async () => {
  const info = await dumpReader(await conn.getInfo())

  assert.strictEqual(info[0].info_name, 0)
  assert.strictEqual(info[0].info_value, 'SQLite')
})
