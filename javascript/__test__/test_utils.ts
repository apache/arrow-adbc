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

import { RecordBatchReader } from 'apache-arrow'
import { AdbcDatabase, AdbcStatement } from '../lib/index.js'

export async function createSqliteDatabase(): Promise<AdbcDatabase> {
  const driver = process.env.ADBC_DRIVER_MANAGER_TEST_LIB ?? 'sqlite'
  return new AdbcDatabase({ driver })
}

export async function createTestTable(stmt: AdbcStatement, tableName: string = 'test_table'): Promise<void> {
  try {
    await stmt.setSqlQuery(`DROP TABLE IF EXISTS ${tableName}`)
    await stmt.executeUpdate()
  } catch {
    // Ignore errors
  }
  await stmt.setSqlQuery(`CREATE TABLE ${tableName} (id INTEGER, name TEXT)`)
  await stmt.executeUpdate()
}

export async function dumpReader(reader: RecordBatchReader): Promise<any[]> {
  const rows: any[] = []
  for await (const batch of reader) {
    for (const row of batch) {
      rows.push(deepUnwrap(row?.toJSON()))
    }
  }
  return rows
}

function deepUnwrap(obj: any): any {
  if (obj === null || obj === undefined) return obj

  if (Array.isArray(obj)) {
    return obj.map(deepUnwrap)
  }

  // Heuristic to identify Arrow Vectors: iterable and has a 'get' method
  if (
    typeof obj === 'object' &&
    obj !== null &&
    typeof obj[Symbol.iterator] === 'function' &&
    typeof obj.get === 'function'
  ) {
    return [...obj].map(deepUnwrap)
  }

  if (typeof obj === 'object') {
    const result: any = {}
    for (const key of Object.keys(obj)) {
      result[key] = deepUnwrap(obj[key])
    }
    return result
  }

  return obj
}
