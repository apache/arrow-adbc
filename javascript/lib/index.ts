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

import { NativeAdbcDatabase, NativeAdbcConnection, NativeAdbcStatement } from '../binding.js'

import type {
  AdbcDatabase as AdbcDatabaseInterface,
  AdbcConnection as AdbcConnectionInterface,
  AdbcStatement as AdbcStatementInterface,
  ConnectOptions,
  GetObjectsOptions,
  IngestOptions,
} from './types.js'
import { LoadFlags, ObjectDepth, InfoCode, IngestMode } from './types.js'

import { RecordBatch, RecordBatchReader, Table, tableToIPC, Schema } from 'apache-arrow'
import { AdbcError } from './error.js'

// Safely define Symbol.asyncDispose for compatibility with Node.js environments older than v21.
const asyncDisposeSymbol = (Symbol as any).asyncDispose ?? Symbol('Symbol.asyncDispose')

type NativeIterator = { next(): Promise<Buffer | null | undefined>; close(): void }

async function readerToTable(reader: RecordBatchReader): Promise<Table> {
  const batches: RecordBatch[] = []
  for await (const batch of reader) {
    batches.push(batch)
  }
  return new Table(batches)
}

/**
 * Converts the native result iterator into an Apache Arrow `RecordBatchReader`.
 *
 * The native iterator yields Arrow IPC stream bytes (schema + record batches) as a
 * Node.js `Buffer`. Since `Buffer` is a subclass of `Uint8Array`, we can wrap the
 * iterator directly as `AsyncIterable<Uint8Array>` and pass it to
 * `RecordBatchReader.from()`, which accepts an async iterable of IPC bytes and
 * returns a standard Arrow `RecordBatchReader`.
 */
async function iteratorToReader(iterator: NativeIterator): Promise<RecordBatchReader> {
  const asyncIterable: AsyncIterable<Uint8Array> = {
    [Symbol.asyncIterator]: async function* () {
      try {
        while (true) {
          const chunk = await iterator.next().catch((e) => {
            throw AdbcError.fromError(e)
          })
          if (!chunk) {
            break
          }
          yield chunk as Uint8Array
        }
      } finally {
        try {
          iterator.close()
        } catch {}
      }
    },
  }
  return RecordBatchReader.from(asyncIterable)
}

// Export Options types, constants, and Error class
export type { ConnectOptions, GetObjectsOptions, IngestOptions }
export { AdbcError, LoadFlags, ObjectDepth, InfoCode, IngestMode }

/**
 * Represents an ADBC Database.
 */
export class AdbcDatabase implements AdbcDatabaseInterface {
  private _inner: NativeAdbcDatabase

  constructor(options: ConnectOptions) {
    try {
      this._inner = new NativeAdbcDatabase(options)
    } catch (e) {
      throw AdbcError.fromError(e)
    }
  }

  async connect(): Promise<AdbcConnection> {
    try {
      const connInner = await this._inner.connect(null)
      return new AdbcConnection(connInner as NativeAdbcConnection)
    } catch (e) {
      throw AdbcError.fromError(e)
    }
  }

  async close(): Promise<void> {
    try {
      await this._inner.close()
    } catch (e) {
      throw AdbcError.fromError(e)
    }
  }

  async [asyncDisposeSymbol](): Promise<void> {
    return this.close()
  }
}

/**
 * Represents a single connection to a database.
 */
export class AdbcConnection implements AdbcConnectionInterface {
  private _inner: NativeAdbcConnection

  constructor(inner: NativeAdbcConnection) {
    this._inner = inner
  }

  async createStatement(): Promise<AdbcStatement> {
    try {
      const stmtInner = await this._inner.createStatement()
      return new AdbcStatement(stmtInner as NativeAdbcStatement)
    } catch (e) {
      throw AdbcError.fromError(e)
    }
  }

  setOption(key: string, value: string): void {
    try {
      this._inner.setOption(key, value)
    } catch (e) {
      throw AdbcError.fromError(e)
    }
  }

  setAutoCommit(enabled: boolean): void {
    this.setOption('adbc.connection.autocommit', enabled ? 'true' : 'false')
  }

  setReadOnly(enabled: boolean): void {
    this.setOption('adbc.connection.read_only', enabled ? 'true' : 'false')
  }

  async getObjects(options?: GetObjectsOptions): Promise<Table> {
    try {
      const opts = {
        depth: options?.depth ?? 0,
        catalog: options?.catalog,
        dbSchema: options?.dbSchema,
        tableName: options?.tableName,
        tableType: options?.tableType,
        columnName: options?.columnName,
      }
      const iterator = await this._inner.getObjects(opts)
      return readerToTable(await iteratorToReader(iterator as NativeIterator))
    } catch (e) {
      throw AdbcError.fromError(e)
    }
  }

  async getTableSchema(options: { catalog?: string; dbSchema?: string; tableName: string }): Promise<Schema> {
    try {
      const buffer = await this._inner.getTableSchema(options)
      const reader = RecordBatchReader.from(buffer as Uint8Array)
      if (!reader.schema) {
        await reader.next()
      }
      return reader.schema
    } catch (e) {
      throw AdbcError.fromError(e)
    }
  }

  async getTableTypes(): Promise<Table> {
    try {
      const iterator = await this._inner.getTableTypes()
      return readerToTable(await iteratorToReader(iterator as NativeIterator))
    } catch (e) {
      throw AdbcError.fromError(e)
    }
  }

  async getInfo(infoCodes?: InfoCode[]): Promise<Table> {
    try {
      const iterator = await this._inner.getInfo(infoCodes)
      return readerToTable(await iteratorToReader(iterator as NativeIterator))
    } catch (e) {
      throw AdbcError.fromError(e)
    }
  }

  async query(sql: string, params?: Table): Promise<Table> {
    return readerToTable(await this.queryStream(sql, params))
  }

  async queryStream(sql: string, params?: Table): Promise<RecordBatchReader> {
    const stmt = await this.createStatement()
    try {
      await stmt.setSqlQuery(sql)
      if (params !== undefined) {
        await stmt.bind(params)
      }
      return await stmt.executeQuery()
    } finally {
      await stmt.close()
    }
  }

  private setIngestOptions(
    stmt: { setOption(key: string, value: string): void },
    tableName: string,
    options?: IngestOptions,
  ): void {
    stmt.setOption('adbc.ingest.target_table', tableName)
    stmt.setOption('adbc.ingest.mode', options?.mode ?? IngestMode.Create)
    if (options?.catalog !== undefined) {
      stmt.setOption('adbc.ingest.target_catalog', options.catalog)
    }
    if (options?.dbSchema !== undefined) {
      stmt.setOption('adbc.ingest.target_db_schema', options.dbSchema)
    }
    if (options?.temporary === true) {
      stmt.setOption('adbc.ingest.temporary', 'true')
    }
  }

  async ingest(tableName: string, data: Table, options?: IngestOptions): Promise<number> {
    const stmt = await this.createStatement()
    try {
      this.setIngestOptions(stmt, tableName, options)
      await stmt.bind(data)
      return await stmt.executeUpdate()
    } finally {
      await stmt.close()
    }
  }

  async ingestStream(tableName: string, reader: RecordBatchReader, options?: IngestOptions): Promise<number> {
    const nativeStmt = (await this._inner.createStatement()) as NativeAdbcStatement
    try {
      this.setIngestOptions(nativeStmt, tableName, options)

      await reader.open()
      const schemaTable = new Table(reader.schema, [])
      const schemaBytes = tableToIPC(schemaTable, 'stream')
      const promise = nativeStmt.startBindStreamExecute(Buffer.from(schemaBytes))

      let pushError: unknown
      try {
        for await (const batch of reader) {
          const batchBytes = tableToIPC(new Table([batch]), 'stream')
          nativeStmt.pushBatch(Buffer.from(batchBytes))
        }
      } catch (e) {
        pushError = e
      } finally {
        nativeStmt.endStream()
      }

      const result = (await promise) as number
      if (pushError) throw pushError
      return result
    } catch (e) {
      throw AdbcError.fromError(e)
    } finally {
      nativeStmt.close()
    }
  }

  async execute(sql: string, params?: Table): Promise<number> {
    const stmt = await this.createStatement()
    try {
      await stmt.setSqlQuery(sql)
      if (params !== undefined) {
        await stmt.bind(params)
      }
      return await stmt.executeUpdate()
    } finally {
      await stmt.close()
    }
  }

  async commit(): Promise<void> {
    try {
      await this._inner.commit()
    } catch (e) {
      throw AdbcError.fromError(e)
    }
  }

  async rollback(): Promise<void> {
    try {
      await this._inner.rollback()
    } catch (e) {
      throw AdbcError.fromError(e)
    }
  }

  async close(): Promise<void> {
    try {
      await this._inner.close()
    } catch (e) {
      throw AdbcError.fromError(e)
    }
  }

  async [asyncDisposeSymbol](): Promise<void> {
    return this.close()
  }
}

/**
 * Represents a query statement.
 */
export class AdbcStatement implements AdbcStatementInterface {
  private _inner: NativeAdbcStatement

  constructor(inner: NativeAdbcStatement) {
    this._inner = inner
  }

  async setSqlQuery(query: string): Promise<void> {
    try {
      this._inner.setSqlQuery(query)
    } catch (e) {
      throw AdbcError.fromError(e)
    }
  }

  setOption(key: string, value: string): void {
    try {
      this._inner.setOption(key, value)
    } catch (e) {
      throw AdbcError.fromError(e)
    }
  }

  /**
   * Execute the query and return a RecordBatchReader.
   *
   * Per the ADBC spec, behavior is undefined if the statement is reused after
   * calling executeQuery(). Create a new statement for each query.
   */
  async executeQuery(): Promise<RecordBatchReader> {
    try {
      const iterator = await this._inner.executeQuery()
      return iteratorToReader(iterator as NativeIterator)
    } catch (e) {
      throw AdbcError.fromError(e)
    }
  }

  async executeUpdate(): Promise<number> {
    try {
      const rows = await this._inner.executeUpdate()
      return rows as number
    } catch (e) {
      throw AdbcError.fromError(e)
    }
  }

  async bind(data: Table): Promise<void> {
    try {
      const ipcBytes = tableToIPC(data, 'stream')
      await this._inner.bind(Buffer.from(ipcBytes))
    } catch (e) {
      throw AdbcError.fromError(e)
    }
  }

  async bindStream(reader: RecordBatchReader): Promise<void> {
    try {
      await reader.open()
      const schemaTable = new Table(reader.schema, [])
      const schemaBytes = tableToIPC(schemaTable, 'stream')
      const bindPromise = this._inner.startBindStream(Buffer.from(schemaBytes))

      let pushError: unknown
      try {
        for await (const batch of reader) {
          const batchBytes = tableToIPC(new Table([batch]), 'stream')
          this._inner.pushBatch(Buffer.from(batchBytes))
        }
      } catch (e) {
        pushError = e
      } finally {
        this._inner.endStream()
      }

      await bindPromise
      if (pushError) throw pushError
    } catch (e) {
      throw AdbcError.fromError(e)
    }
  }

  async close(): Promise<void> {
    try {
      await this._inner.close()
    } catch (e) {
      throw AdbcError.fromError(e)
    }
  }

  async [asyncDisposeSymbol](): Promise<void> {
    return this.close()
  }
}
