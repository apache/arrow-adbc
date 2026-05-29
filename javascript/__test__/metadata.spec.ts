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
import { Table } from 'apache-arrow'

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
  const table = await conn.getTableTypes()
  assert.ok(table instanceof Table)

  const types = Array.from({ length: table.numRows }, (_, i) => table.getChild('table_type')?.get(i) as string)
  types.sort()

  assert.ok(types.includes('table'))
  assert.ok(types.includes('view'))
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
  const table = await conn.getObjects({
    depth: 3,
    tableName: 'metadata_test',
    tableType: ['table', 'view'],
  })
  assert.ok(table instanceof Table)
  assert.ok(table.numRows > 0)

  // Navigate the nested Arrow structure: catalog -> db_schemas -> tables
  const dbSchemas = table.getChild('catalog_db_schemas')?.get(0)
  const dbTables = dbSchemas?.get(0)?.db_schema_tables
  const tableNames = Array.from({ length: dbTables?.length ?? 0 }, (_, i) => dbTables?.get(i)?.table_name as string)
  assert.ok(tableNames.includes('metadata_test'))
})

test('metadata: getInfo', async () => {
  const table = await conn.getInfo()
  assert.ok(table instanceof Table)
  assert.ok(table.numRows > 0)

  // Find the VendorName row (info_name === 0)
  const infoNames = table.getChild('info_name')
  const vendorNameRow = Array.from({ length: table.numRows }, (_, i) => i).find((i) => infoNames?.get(i) === 0)
  assert.ok(vendorNameRow !== undefined)
  assert.strictEqual(table.getChild('info_value')?.get(vendorNameRow), 'SQLite')
})
