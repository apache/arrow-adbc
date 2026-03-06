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
 * Bitmask flags controlling how the driver manager resolves a driver name.
 *
 * These values correspond to the `ADBC_LOAD_FLAG_*` constants in the ADBC spec.
 * Flags can be combined with bitwise OR. When `loadFlags` is omitted in
 * `ConnectOptions`, `LoadFlags.Default` is used.
 *
 * @example
 * // Only search system paths, disallow relative paths
 * const db = new AdbcDatabase({
 *   driver: 'sqlite',
 *   loadFlags: LoadFlags.SearchSystem,
 * })
 */
export const LoadFlags = {
  /** Search directory paths in the `ADBC_DRIVER_PATH` environment variable (and the conda environment, if installed via conda). */
  SearchEnv: 1 << 1,
  /** Search the user configuration directory. */
  SearchUser: 1 << 2,
  /** Search the system configuration directory. */
  SearchSystem: 1 << 3,
  /** Allow a relative path to be provided as the driver name. */
  AllowRelativePaths: 1 << 4,
  /** All defined flags enabled. This is the default when `loadFlags` is omitted. */
  Default: (1 << 1) | (1 << 2) | (1 << 3) | (1 << 4),
} as const
export type LoadFlags = (typeof LoadFlags)[keyof typeof LoadFlags]

/**
 * Depth values for the `getObjects` metadata call.
 *
 * These correspond to the `ADBC_OBJECT_DEPTH_*` constants in the ADBC spec.
 *
 * @example
 * // Retrieve catalogs and schemas only
 * await conn.getObjects({ depth: ObjectDepth.Schemas })
 */
export const ObjectDepth = {
  /** Catalogs, schemas, tables, and columns (default). */
  All: 0,
  /** Catalogs only. */
  Catalogs: 1,
  /** Catalogs and schemas. */
  Schemas: 2,
  /** Catalogs, schemas, and tables. */
  Tables: 3,
} as const
export type ObjectDepth = (typeof ObjectDepth)[keyof typeof ObjectDepth]

/**
 * Info codes for the `getInfo` metadata call.
 *
 * These correspond to the `ADBC_INFO_*` constants in the ADBC spec.
 * Pass a subset to `getInfo()` to retrieve only specific metadata fields.
 *
 * @example
 * const reader = await conn.getInfo([InfoCode.VendorName, InfoCode.DriverVersion])
 */
export const InfoCode = {
  /** The database vendor/product name (string). */
  VendorName: 0,
  /** The database vendor/product version (string). */
  VendorVersion: 1,
  /** The Arrow library version used by the vendor (string). */
  VendorArrowVersion: 2,
  /** Whether the vendor supports SQL queries (bool). */
  VendorSql: 3,
  /** Whether the vendor supports Substrait queries (bool). */
  VendorSubstrait: 4,
  /** Minimum supported Substrait version, or null (string). */
  VendorSubstraitMinVersion: 5,
  /** Maximum supported Substrait version, or null (string). */
  VendorSubstraitMaxVersion: 6,
  /** The driver name (string). */
  DriverName: 100,
  /** The driver version (string). */
  DriverVersion: 101,
  /** The Arrow library version used by the driver (string). */
  DriverArrowVersion: 102,
  /** The ADBC API version implemented by the driver (int64). Available since ADBC 1.1.0. */
  DriverAdbcVersion: 103,
} as const
export type InfoCode = (typeof InfoCode)[keyof typeof InfoCode]

/**
 * Options for connecting to a driver/database.
 *
 * These options configure how the ADBC driver is loaded and how the initial connection is established.
 */
export interface ConnectOptions {
  /**
   * Driver to load. Accepts any of the following forms:
   * - Short name: `"sqlite"`, `"postgresql"` — the driver manager searches for a matching
   *   manifest file (e.g. `sqlite.toml`) in the configured directories, then falls back to
   *   `LD_LIBRARY_PATH` / `PATH`.
   * - Absolute path to a shared library: `"/usr/lib/libadbc_driver_sqlite.so"`
   * - Absolute path to a driver manifest `.toml` file (with or without the `.toml` extension).
   * - Relative path (only valid when {@link LoadFlags.AllowRelativePaths} is set).
   *
   * **Note:** URI-style driver strings (e.g. `"sqlite:file::memory:"`, `"postgresql://..."`)
   * and connection profile URIs (e.g. `"profile://my_profile"`) are defined in the ADBC spec
   * but are not yet supported. Use `databaseOptions` to pass a connection URI to the driver instead:
   * ```ts
   * new AdbcDatabase({ driver: 'sqlite', databaseOptions: { uri: 'file::memory:' } })
   * ```
   */
  driver: string
  /**
   * Name of the entrypoint function (optional).
   * If not provided, ADBC will attempt to guess the entrypoint symbol name based on the driver name.
   */
  entrypoint?: string
  /**
   * Additional directories to search for drivers and driver manifest (`.toml`) profile files (optional).
   * Searched before the default system and user configuration directories.
   */
  searchPaths?: string[]
  /**
   * Bitmask controlling how the driver name is resolved (optional).
   * Use the {@link LoadFlags} constants to compose a value.
   * Defaults to {@link LoadFlags.Default} (all search locations enabled) when omitted.
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
   * The level of depth to retrieve. Use the {@link ObjectDepth} constants.
   * Defaults to {@link ObjectDepth.All} when omitted.
   */
  depth?: ObjectDepth
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
   * @param infoCodes Optional list of info codes to retrieve. Use the {@link InfoCode} constants.
   *   If omitted, all available info is returned.
   * @returns A RecordBatchReader containing the requested metadata info.
   */
  getInfo(infoCodes?: InfoCode[]): Promise<RecordBatchReader>

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
