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

import { RecordBatch, RecordBatchReader, Table, Schema } from 'apache-arrow'

/**
 * Options for connecting to a driver/database.
 *
 * These options configure how the ADBC driver is loaded and how the initial connection is established.
 */
export interface ConnectOptions {
  /**
   * Path to the driver library or short name of the driver.
   * Short names (e.g. "sqlite") work when the corresponding ADBC driver is installed on the system.
   * Full paths to a .so/.dylib/.dll or a manifest .toml file are also accepted.
   */
  driver: string
  /**
   * Name of the entrypoint function (optional).
   * If not provided, ADBC will attempt to guess the entrypoint symbol name based on the driver name.
   */
  entrypoint?: string
  /**
   * Paths to search for the driver (optional).
   * Useful if the driver is not in the standard library paths.
   */
  searchPaths?: string[]
  /**
   * Load flags (optional).
   * Bitmask controlling how the driver is loaded (e.g., search system paths, search user paths).
   */
  loadFlags?: number
  /**
   * Database-specific options.
   * Key-value pairs passed to the driver during database initialization (e.g., "uri", "username").
   */
  databaseOptions?: Record<string, string>
}

/** Options for getObjects metadata call. */
export interface GetObjectsOptions {
  /**
   * The level of depth to retrieve.
   * 0: All (Catalogs, Schemas, Tables, and Columns)
   * 1: Catalogs only
   * 2: Catalogs and Schemas
   * 3: Catalogs, Schemas, and Tables
   * 4: Catalogs, Schemas, Tables, and Columns (same as 0)
   */
  depth?: number
  /** Filter by catalog name pattern. */
  catalog?: string
  /** Filter by database schema name pattern. */
  dbSchema?: string
  /** Filter by table name pattern. */
  tableName?: string
  /** Filter by table type (e.g., ["table", "view"]). */
  tableType?: string[]
  /** Filter by column name pattern. */
  columnName?: string
}

/**
 * Represents an ADBC Database.
 *
 * An AdbcDatabase represents a handle to a database. This may be a single file (SQLite),
 * a connection configuration (PostgreSQL), or an in-memory database.
 * It holds state that is shared across multiple connections.
 */
export interface AdbcDatabase {
  /**
   * Open a new connection to the database.
   *
   * @returns A Promise resolving to a new AdbcConnection.
   */
  connect(): Promise<AdbcConnection>

  /**
   * Release the database resources.
   * After closing, the database object should not be used.
   */
  close(): Promise<void>
}

/**
 * Represents a single connection to a database.
 *
 * An AdbcConnection maintains the state of a connection to the database, such as
 * current transaction state and session options.
 */
export interface AdbcConnection {
  /**
   * Create a new statement for executing queries.
   *
   * @returns A Promise resolving to a new AdbcStatement.
   */
  createStatement(): Promise<AdbcStatement>

  /**
   * Set an option on the connection.
   *
   * @param key The option name (e.g., "adbc.connection.autocommit").
   * @param value The option value.
   */
  setOption(key: string, value: string): void

  /**
   * Toggle autocommit behavior.
   *
   * @param enabled Whether autocommit should be enabled.
   */
  setAutoCommit(enabled: boolean): void

  /**
   * Toggle read-only mode.
   *
   * @param enabled Whether the connection should be read-only.
   */
  setReadOnly(enabled: boolean): void

  /**
   * Get a hierarchical view of database objects (catalogs, schemas, tables, columns).
   *
   * @param options Filtering options for the metadata query.
   * @returns A RecordBatchReader containing the metadata.
   */
  getObjects(options?: GetObjectsOptions): Promise<RecordBatchReader>

  /**
   * Get the Arrow schema for a specific table.
   *
   * @param options An object containing catalog, dbSchema, and tableName.
   * @param options.catalog The catalog name (or undefined).
   * @param options.dbSchema The schema name (or undefined).
   * @param options.tableName The table name.
   * @returns A Promise resolving to the Arrow Schema of the table.
   */
  getTableSchema(options: { catalog?: string; dbSchema?: string; tableName: string }): Promise<Schema>

  /**
   * Get a list of table types supported by the database.
   *
   * @returns A RecordBatchReader containing a single string column of table types.
   */
  getTableTypes(): Promise<RecordBatchReader>

  /**
   * Get metadata about the driver and database.
   *
   * @param infoCodes Optional list of integer info codes to retrieve.
   * @returns A RecordBatchReader containing the requested metadata info.
   */
  getInfo(infoCodes?: number[]): Promise<RecordBatchReader>

  /**
   * Execute a SQL query and return the results as a RecordBatchReader.
   *
   * Convenience method that creates a statement, sets the SQL, optionally binds
   * parameters, executes the query, and closes the statement. The reader remains
   * valid after the statement is closed because the underlying iterator holds its
   * own reference to the native resources.
   *
   * @param sql The SQL query to execute.
   * @param params Optional Arrow RecordBatch or Table to bind as parameters.
   * @returns A Promise resolving to an Apache Arrow RecordBatchReader.
   */
  query(sql: string, params?: RecordBatch | Table): Promise<RecordBatchReader>

  /**
   * Execute a SQL statement (INSERT, UPDATE, DELETE, DDL) and return the row count.
   *
   * Convenience method that creates a statement, sets the SQL, optionally binds
   * parameters, executes the update, and closes the statement.
   *
   * @param sql The SQL statement to execute.
   * @param params Optional Arrow RecordBatch or Table to bind as parameters.
   * @returns A Promise resolving to the number of rows affected, or -1 if unknown.
   */
  execute(sql: string, params?: RecordBatch | Table): Promise<number>

  /**
   * Commit any pending transactions.
   * Only valid if autocommit is disabled.
   */
  commit(): Promise<void>

  /**
   * Rollback any pending transactions.
   * Only valid if autocommit is disabled.
   */
  rollback(): Promise<void>

  /**
   * Close the connection and release resources.
   */
  close(): Promise<void>
}

/**
 * Represents a query statement.
 *
 * An AdbcStatement is used to execute SQL queries or prepare bulk insertions.
 * State such as the SQL query string or bound parameters is held by the statement.
 */
export interface AdbcStatement {
  /**
   * Set the SQL query string.
   *
   * @param query The SQL query to execute.
   */
  setSqlQuery(query: string): Promise<void>

  /**
   * Set an option on the statement.
   *
   * @param key The option name (e.g., "adbc.ingest.target_table").
   * @param value The option value.
   */
  setOption(key: string, value: string): void

  /**
   * Execute the query and return a stream of results.
   *
   * @returns A Promise resolving to an Apache Arrow RecordBatchReader.
   * The reader must be consumed or closed to release resources.
   */
  executeQuery(): Promise<RecordBatchReader>

  /**
   * Execute an update command (e.g., INSERT, UPDATE, DELETE) that returns no data.
   *
   * @returns A Promise resolving to the number of rows affected (if known), or -1.
   */
  executeUpdate(): Promise<number>

  /**
   * Bind parameters or data for ingestion.
   *
   * This binds an Arrow RecordBatch or Table to the statement.
   * This is used for bulk ingestion or parameterized queries.
   *
   * @param data Arrow RecordBatch or Table containing the data to bind.
   */
  bind(data: RecordBatch | Table): Promise<void>

  /**
   * Close the statement and release resources.
   */
  close(): Promise<void>
}
