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

import { test } from 'node:test'
import assert from 'node:assert/strict'
import { mkdtempSync, writeFileSync, rmSync } from 'node:fs'
import { join, isAbsolute } from 'node:path'
import { tmpdir } from 'node:os'
import { AdbcDatabase } from '../lib/index.js'
import { dumpReader } from './test_utils.js'

const testLib = process.env.ADBC_DRIVER_MANAGER_TEST_LIB

test('profile: load database from profile:// URI', async () => {
  const driver = testLib ?? 'sqlite'
  const tmpDir = mkdtempSync(join(tmpdir(), 'adbc-profile-test-'))
  try {
    // Create a SQLite file with a known table so we can confirm the profile
    // is actually pointing us at the right database.
    const dbPath = join(tmpDir, 'test.db')
    const setupDb = new AdbcDatabase({ driver, databaseOptions: { uri: dbPath } })
    const setupConn = await setupDb.connect()
    await setupConn.execute('CREATE TABLE profile_marker (id INTEGER, value TEXT)')
    await setupConn.execute("INSERT INTO profile_marker VALUES (42, 'from_profile')")
    await setupConn.close()
    await setupDb.close()

    // TOML requires forward slashes — backslashes are escape sequences
    const toml = (p: string) => p.replaceAll('\\', '/')
    writeFileSync(
      join(tmpDir, 'test_sqlite.toml'),
      `version = 1\ndriver = "${toml(driver)}"\n\n[options]\nuri = "${toml(dbPath)}"\n`,
    )

    const db = new AdbcDatabase({
      driver: 'profile://test_sqlite',
      searchPaths: [tmpDir],
    })
    const conn = await db.connect()

    const rows = await dumpReader(await conn.query('SELECT id, value FROM profile_marker'))
    assert.strictEqual(rows.length, 1)
    assert.strictEqual(rows[0].id, 42n)
    assert.strictEqual(rows[0].value, 'from_profile')

    await conn.close()
    await db.close()
  } finally {
    rmSync(tmpDir, { recursive: true })
  }
})

// URI-style driver strings require a short name as the scheme — skip when the
// test env var is set to an absolute path (e.g. on Windows CI).
test('profile: load database from sqlite: URI', { skip: testLib !== undefined && isAbsolute(testLib) }, async () => {
  const driver = testLib ?? 'sqlite'
  const db = new AdbcDatabase({
    driver: `${driver}::memory:`,
  })
  const conn = await db.connect()

  await conn.query('SELECT 1 AS n')

  await conn.close()
  await db.close()
})
