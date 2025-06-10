/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using System.Runtime.InteropServices;
using Apache.Arrow.C;

namespace Apache.Arrow.Adbc.C
{
    /// <summary>
    /// An instance of an initialized database driver.
    /// </summary>
    /// <remarks>
    /// This provides a common interface for vendor-specific driver
    /// initialization routines. Drivers should populate this struct, and
    /// applications can call ADBC functions through this struct, without
    /// worrying about multiple definitions of the same symbol.
    /// </remarks>
    [StructLayout(LayoutKind.Sequential)]
    public unsafe struct CAdbcDriver
    {
        #region ADBC API Revision 1.0.0

        /// <summary>
        /// Opaque driver-defined state.
        /// This field is NULL if the driver is uninitialized/freed (but
        /// it need not have a value even if the driver is initialized).
        /// </summary>
        public void* private_data;

        /// <summary>
        /// Opaque driver manager-defined state.
        /// This field is NULL if the driver is uninitialized/freed (but
        /// it need not have a value even if the driver is initialized).
        /// </summary>
        public void* private_manager;

        /// <summary>
        /// Release the driver and perform any cleanup.
        ///
        /// This is an embedded callback to make it easier for the driver
        /// manager and driver to cooperate.
        /// </summary>
#if NET5_0_OR_GREATER
        internal delegate* unmanaged<CAdbcDriver*, CAdbcError*, AdbcStatusCode> release;
#else
        internal IntPtr release;
#endif

        /// <summary>
        /// Finish setting options and initialize the database.
        ///
        /// Some drivers may support setting options after initialization
        /// as well.
        /// </summary>
#if NET5_0_OR_GREATER
        internal delegate* unmanaged<CAdbcDatabase*, CAdbcError*, AdbcStatusCode> DatabaseInit;
#else
        internal IntPtr DatabaseInit;
#endif

        /// <summary>
        /// Allocate a new (but uninitialized) database.
        ///
        /// Callers pass in a zero-initialized AdbcDatabase.
        ///
        /// Drivers should allocate their internal data structure and set
        /// the private_data field to point to the newly allocated struct.
        /// This struct should be released when AdbcDatabaseRelease is called.
        /// </summary>
#if NET5_0_OR_GREATER
        internal delegate* unmanaged<CAdbcDatabase*, CAdbcError*, AdbcStatusCode> DatabaseNew;
#else
        internal IntPtr DatabaseNew;
#endif

        /// <summary>
        /// Set a byte* option on the database.
        ///
        /// Options may be set before AdbcDatabaseInit.  Some drivers may
        /// support setting options after initialization as well.
        ///
        /// </summary>
#if NET5_0_OR_GREATER
        internal delegate* unmanaged<CAdbcDatabase*, byte*, byte*, CAdbcError*, AdbcStatusCode> DatabaseSetOption;
#else
        internal IntPtr DatabaseSetOption;
#endif

        /// <summary>
        /// Destroy this database. No connections may exist.
        /// </summary>
#if NET5_0_OR_GREATER
        internal delegate* unmanaged<CAdbcDatabase*, CAdbcError*, AdbcStatusCode> DatabaseRelease;
#else
        internal IntPtr DatabaseRelease;
#endif

        /// <summary>
        /// Commit any pending transactions. Only used if autocommit is
        /// disabled.
        ///
        /// Behavior is undefined if this is mixed with SQL transaction
        /// statements.
        /// </summary>
#if NET5_0_OR_GREATER
        internal delegate* unmanaged<CAdbcConnection*, CAdbcError*, AdbcStatusCode> ConnectionCommit;
#else
        internal IntPtr ConnectionCommit;
#endif

        /// <summary>
        /// Get metadata about the database/driver.
        ///
        /// The result is an Arrow dataset with the following schema:
        ///
        /// Field Name                  | Field Type
        /// ----------------------------|------------------------
        /// info_name                   | uint32 not null
        /// info_value                  | INFO_SCHEMA
        ///
        /// INFO_SCHEMA is a dense union with members:
        ///
        /// Field Name (Type Code)      | Field Type
        /// ----------------------------|------------------------
        /// string_value (0)            | utf8
        /// bool_value (1)              | bool
        /// int64_value (2)             | int64
        /// int32_bitmask (3)           | int32
        /// string_list (4)             | list<utf8>
        /// int32_to_int32_list_map (5) | map<int32, list<int32>>
        ///
        /// Each metadatum is identified by an integer code.  The recognized
        /// codes are defined as constants.  Codes [0, 10_000) are reserved
        /// for ADBC usage.  Drivers/vendors will ignore requests for
        /// unrecognized codes (the row will be omitted from the result).
        /// </summary>
#if NET5_0_OR_GREATER
        internal delegate* unmanaged<CAdbcConnection*, int*, int, CArrowArrayStream*, CAdbcError*, AdbcStatusCode> ConnectionGetInfo;
#else
        internal IntPtr ConnectionGetInfo;
#endif

        /// <summary>
        ///  Get a hierarchical view of all catalogs, database schemas,
        ///   tables, and columns.
        ///
        /// The result is an Arrow dataset with the following schema:
        ///
        /// | Field Name               | Field Type              |
        /// |--------------------------|-------------------------|
        /// | catalog_name             | utf8                    |
        /// | catalog_db_schemas       | list<DB_SCHEMA_SCHEMA>  |
        ///
        /// DB_SCHEMA_SCHEMA is a Struct with fields:
        ///
        /// | Field Name               | Field Type              |
        /// |--------------------------|-------------------------|
        /// | db_schema_name           | utf8                    |
        /// | db_schema_tables         | list<TABLE_SCHEMA>      |
        ///
        /// TABLE_SCHEMA is a Struct with fields:
        ///
        /// | Field Name               | Field Type              |
        /// |--------------------------|-------------------------|
        /// | table_name               | utf8 not null           |
        /// | table_type               | utf8 not null           |
        /// | table_columns            | list<COLUMN_SCHEMA>     |
        /// | table_constraints        | list<CONSTRAINT_SCHEMA> |
        /// </summary>
#if NET5_0_OR_GREATER
        internal delegate* unmanaged<CAdbcConnection*, int, byte*, byte*, byte*, byte**, byte*, CArrowArrayStream*, CAdbcError*, AdbcStatusCode> ConnectionGetObjects;
#else
        internal IntPtr ConnectionGetObjects;
#endif

        /// <summary>
        /// Get the Arrow schema of a table.
        /// </summary>
#if NET5_0_OR_GREATER
        internal delegate* unmanaged<CAdbcConnection*, byte*, byte*, byte*, CArrowSchema*, CAdbcError*, AdbcStatusCode> ConnectionGetTableSchema;
#else
        internal IntPtr ConnectionGetTableSchema;
#endif

        /// <summary>
        /// Get a list of table types in the database.
        ///
        /// The result is an Arrow dataset with the following schema:
        ///
        /// Field Name     | Field Type
        /// ---------------|--------------
        /// table_type     | utf8 not null
        /// </summary>
#if NET5_0_OR_GREATER
        internal delegate* unmanaged<CAdbcConnection*, CArrowArrayStream*, CAdbcError*, AdbcStatusCode> ConnectionGetTableTypes;
#else
        internal IntPtr ConnectionGetTableTypes;
#endif

        /// <summary>
        /// Finish setting options and initialize the connection.
        ///
        /// Some drivers may support setting options after initialization
        /// as well.
        /// </summary>
#if NET5_0_OR_GREATER
        internal delegate* unmanaged<CAdbcConnection*, CAdbcDatabase*, CAdbcError*, AdbcStatusCode> ConnectionInit;
#else
        internal IntPtr ConnectionInit;
#endif

        /// <summary>
        /// Allocate a new (but uninitialized) connection.
        ///
        /// Callers pass in a zero-initialized AdbcConnection.
        ///
        /// Drivers should allocate their internal data structure and set
        /// the private_data field to point to the newly allocated struct.
        /// This struct should be released when AdbcConnectionRelease is
        /// called.
        /// </summary>
#if NET5_0_OR_GREATER
        internal delegate* unmanaged<CAdbcConnection*, CAdbcError*, AdbcStatusCode> ConnectionNew;
#else
        internal IntPtr ConnectionNew;
#endif

        /// <summary>
        /// Set a byte* option on the connection.
        ///
        /// Options may be set before AdbcConnectionInit.  Some  drivers may
        /// support setting options after initialization as well.
        /// </summary>
#if NET5_0_OR_GREATER
        internal delegate* unmanaged<CAdbcConnection*, byte*, byte*, CAdbcError*, AdbcStatusCode> ConnectionSetOption;
#else
        internal IntPtr ConnectionSetOption;
#endif

        /// <summary>
        /// Construct a statement for a partition of a query. The
        ///   results can then be read independently.
        ///
        /// A partition can be retrieved from AdbcPartitions.
        /// </summary>
#if NET5_0_OR_GREATER
        internal delegate* unmanaged<CAdbcConnection*, byte*, int, CArrowArrayStream*, CAdbcError*, AdbcStatusCode> ConnectionReadPartition;
#else
        internal IntPtr ConnectionReadPartition;
#endif

        /// <summary>
        /// Destroy this connection.
        /// </summary>
#if NET5_0_OR_GREATER
        internal delegate* unmanaged<CAdbcConnection*, CAdbcError*, AdbcStatusCode> ConnectionRelease;
#else
        internal IntPtr ConnectionRelease;
#endif

        /// <summary>
        /// Roll back any pending transactions. Only used if autocommit is disabled.
        ///
        /// Behavior is undefined if this is mixed with SQL transaction
        /// statements.
        /// </summary>
#if NET5_0_OR_GREATER
        internal delegate* unmanaged<CAdbcConnection*, CAdbcError*, AdbcStatusCode> ConnectionRollback;
#else
        internal IntPtr ConnectionRollback;
#endif

        /// <summary>
        /// Bind Arrow data. This can be used for bulk inserts or prepared
        /// statements.
        /// </summary>
#if NET5_0_OR_GREATER
        internal delegate* unmanaged<CAdbcStatement*, CArrowArray*, CArrowSchema*, CAdbcError*, AdbcStatusCode> StatementBind;
#else
        internal IntPtr StatementBind;
#endif

        /// <summary>
        /// Bind Arrow data. This can be used for bulk inserts or prepared
        /// statements.
        /// </summary>
#if NET5_0_OR_GREATER
        internal delegate* unmanaged<CAdbcStatement*, CArrowArrayStream*, CAdbcError*, AdbcStatusCode> StatementBindStream;
#else
        internal IntPtr StatementBindStream;
#endif

        /// <summary>
        /// Execute a statement and get the results.
        ///
        /// This invalidates any prior result sets.
        /// </summary>
#if NET5_0_OR_GREATER
        internal delegate* unmanaged<CAdbcStatement*, CArrowArrayStream*, long*, CAdbcError*, AdbcStatusCode> StatementExecuteQuery;
#else
        internal IntPtr StatementExecuteQuery;
#endif

        /// <summary>
        /// Execute a statement and get the results as a partitioned result
        /// set.
        /// </summary>
#if NET5_0_OR_GREATER
        internal delegate* unmanaged<CAdbcStatement*, CArrowSchema*, CAdbcPartitions*, long*, CAdbcError*, AdbcStatusCode> StatementExecutePartitions;
#else
        internal IntPtr StatementExecutePartitions;
#endif

        /// <summary>
        /// Get the schema for bound parameters.
        ///
        /// This retrieves an Arrow schema describing the number, names, and
        /// types of the parameters in a parameterized statement.  The fields
        /// of the schema should be in order of the ordinal position of the
        /// parameters; named parameters should appear only once.
        ///
        /// If the parameter does not have a name, or the name cannot be
        /// determined, the name of the corresponding field in the schema
        /// will be an empty string.  If the type cannot be determined,
        /// the type of the corresponding field will be NA (NullType).
        ///
        /// This should be called after AdbcStatementPrepare.
        /// </summary>
#if NET5_0_OR_GREATER
        internal delegate* unmanaged<CAdbcStatement*, CArrowSchema*, CAdbcError*, AdbcStatusCode> StatementGetParameterSchema;
#else
        internal IntPtr StatementGetParameterSchema;
#endif

        /// <summary>
        /// Create a new statement for a given connection.
        ///
        /// Callers pass in a zero-initialized AdbcStatement.
        ///
        /// Drivers should allocate their internal data structure and set
        /// the private_data field to point to the newly allocated struct.
        /// This struct should be released when AdbcStatementRelease is called.
        /// </summary>
#if NET5_0_OR_GREATER
        internal delegate* unmanaged<CAdbcConnection*, CAdbcStatement*, CAdbcError*, AdbcStatusCode> StatementNew;
#else
        internal IntPtr StatementNew;
#endif

        /// <summary>
        /// Turn this statement into a prepared statement to be
        /// executed multiple times.
        ///
        /// This invalidates any prior result sets.
        /// </summary>
#if NET5_0_OR_GREATER
        internal delegate* unmanaged<CAdbcStatement*, CAdbcError*, AdbcStatusCode> StatementPrepare;
#else
        internal IntPtr StatementPrepare;
#endif

        /// <summary>
        /// Destroy a statement.
        /// </summary>
#if NET5_0_OR_GREATER
        internal delegate* unmanaged<CAdbcStatement*, CAdbcError*, AdbcStatusCode> StatementRelease;
#else
        internal IntPtr StatementRelease;
#endif

        /// <summary>
        /// Set a string option on a statement.
        /// </summary>
#if NET5_0_OR_GREATER
        internal delegate* unmanaged<CAdbcStatement*, byte*, byte*, CAdbcError*, AdbcStatusCode> StatementSetOption;
#else
        internal IntPtr StatementSetOption;
#endif

        /// <summary>
        /// Set the SQL query to execute.
        ///
        /// The query can then be executed with StatementExecute.  For
        /// queries expected to be executed repeatedly, StatementPrepare
        /// the statement first.
        /// </summary>
#if NET5_0_OR_GREATER
        internal delegate* unmanaged<CAdbcStatement*, byte*, CAdbcError*, AdbcStatusCode> StatementSetSqlQuery;
#else
        internal IntPtr StatementSetSqlQuery;
#endif

        /// <summary>
        /// Set the Substrait plan to execute.
        ///
        /// The query can then be executed with AdbcStatementExecute.  For
        /// queries expected to be executed repeatedly, AdbcStatementPrepare
        /// the statement first.
        /// </summary>
#if NET5_0_OR_GREATER
        internal delegate* unmanaged<CAdbcStatement*, byte*, int, CAdbcError*, AdbcStatusCode> StatementSetSubstraitPlan;
#else
        internal IntPtr StatementSetSubstraitPlan;
#endif
        #endregion

        #region ADBC API Revision 1.1.0

        /// <summary>
        /// Get the number of metadata values available in an error.
        /// </summary>
#if NET5_0_OR_GREATER
        internal delegate* unmanaged<CAdbcError*, int> ErrorGetDetailCount;
#else
        internal IntPtr ErrorGetDetailCount;
#endif

        /// <summary>
        /// Get a metadata value in an error by index.
        /// </summary>
        /// <remarks>
        /// If an index is invalid, returns an AdbcErrorDetail initialized with null/0 fields.
        /// </remarks>
#if NET5_0_OR_GREATER
        internal delegate* unmanaged<CAdbcError*, int, CAdbcErrorDetail> ErrorGetDetail;
#else
        internal IntPtr ErrorGetDetail;
#endif

        /// <summary>
        /// Get an ADBC error from an ArrowArrayStream created by a driver.
        /// </summary>
        /// <remarks>
        /// This allows retrieving error details and other metadata that would normally be
        /// suppressed by the Arrow C Stream Interface.
        ///
        /// The caller MUST NOT release the error; it is managed by the release callback
        /// in the stream itself.
        /// </remarks>
#if NET5_0_OR_GREATER
        internal delegate* unmanaged<CArrowArrayStream*, AdbcStatusCode*, CAdbcError*> ErrorFromArrayStream;
#else
        internal IntPtr ErrorFromArrayStream;
#endif

        /// <summary>
        /// Get a string option of the database.
        /// </summary>
#if NET5_0_OR_GREATER
        internal delegate* unmanaged<CAdbcDatabase*, byte*, byte*, nint*, CAdbcError*, AdbcStatusCode> DatabaseGetOption;
#else
        internal IntPtr DatabaseGetOption;
#endif

        /// <summary>
        /// Get a byte* option of the database.
        /// </summary>
#if NET5_0_OR_GREATER
        internal delegate* unmanaged<CAdbcDatabase*, byte*, byte*, nint*, CAdbcError*, AdbcStatusCode> DatabaseGetOptionBytes;
#else
        internal IntPtr DatabaseGetOptionBytes;
#endif

        /// <summary>
        /// Get a double option of the database.
        /// </summary>
#if NET5_0_OR_GREATER
        internal delegate* unmanaged<CAdbcDatabase*, byte*, double*, CAdbcError*, AdbcStatusCode> DatabaseGetOptionDouble;
#else
        internal IntPtr DatabaseGetOptionDouble;
#endif

        /// <summary>
        /// Get an integer option of the database.
        /// </summary>
#if NET5_0_OR_GREATER
        internal delegate* unmanaged<CAdbcDatabase*, byte*, long*, CAdbcError*, AdbcStatusCode> DatabaseGetOptionInt;
#else
        internal IntPtr DatabaseGetOptionInt;
#endif

        /// <summary>
        /// Set a byte* option of the database.
        ///
        /// Options may be set before AdbcDatabaseInit.  Some drivers may support setting options after initialization as well.
        /// </summary>
#if NET5_0_OR_GREATER
        internal delegate* unmanaged<CAdbcDatabase*, byte*, byte*, nint, CAdbcError*, AdbcStatusCode> DatabaseSetOptionBytes;
#else
        internal IntPtr DatabaseSetOptionBytes;
#endif

        /// <summary>
        /// Set a double option of the database.
        ///
        /// Options may be set before AdbcDatabaseInit.  Some drivers may support setting options after initialization as well.
        /// </summary>
#if NET5_0_OR_GREATER
        internal delegate* unmanaged<CAdbcDatabase*, byte*, double, CAdbcError*, AdbcStatusCode> DatabaseSetOptionDouble;
#else
        internal IntPtr DatabaseSetOptionDouble;
#endif

        /// <summary>
        /// Set an integer option of the database.
        ///
        /// Options may be set before AdbcDatabaseInit.  Some drivers may support setting options after initialization as well.
        /// </summary>
#if NET5_0_OR_GREATER
        internal delegate* unmanaged<CAdbcDatabase*, byte*, long, CAdbcError*, AdbcStatusCode> DatabaseSetOptionInt;
#else
        internal IntPtr DatabaseSetOptionInt;
#endif

        /// <summary>
        /// Cancel the in-progress operation on a connection.
        /// </summary>
#if NET5_0_OR_GREATER
        internal delegate* unmanaged<CAdbcConnection*, CAdbcError*, AdbcStatusCode> ConnectionCancel;
#else
        internal IntPtr ConnectionCancel;
#endif

        /// <summary>
        /// Get a string option of the connection.
        /// </summary>
#if NET5_0_OR_GREATER
        internal delegate* unmanaged<CAdbcConnection*, byte*, byte*, nint*, CAdbcError*, AdbcStatusCode> ConnectionGetOption;
#else
        internal IntPtr ConnectionGetOption;
#endif

        /// <summary>
        /// Get a byte* option of the connection.
        /// </summary>
#if NET5_0_OR_GREATER
        internal delegate* unmanaged<CAdbcConnection*, byte*, byte*, nint*, CAdbcError*, AdbcStatusCode> ConnectionGetOptionBytes;
#else
        internal IntPtr ConnectionGetOptionBytes;
#endif

        /// <summary>
        /// Get a double option of the connection.
        /// </summary>
#if NET5_0_OR_GREATER
        internal delegate* unmanaged<CAdbcConnection*, byte*, double*, CAdbcError*, AdbcStatusCode> ConnectionGetOptionDouble;
#else
        internal IntPtr ConnectionGetOptionDouble;
#endif

        /// <summary>
        /// Get an integer option of the connection.
        /// </summary>
#if NET5_0_OR_GREATER
        internal delegate* unmanaged<CAdbcConnection*, byte*, long*, CAdbcError*, AdbcStatusCode> ConnectionGetOptionInt;
#else
        internal IntPtr ConnectionGetOptionInt;
#endif

        /// <summary>
        /// Get statistics about the data distribution of table(s). The result is an Arrow dataset.
        /// </summary>
#if NET5_0_OR_GREATER
        internal delegate* unmanaged<CAdbcConnection*, byte*, byte*, byte*, byte, CArrowArrayStream*, CAdbcError*, AdbcStatusCode> ConnectionGetStatistics;
#else
        internal IntPtr ConnectionGetStatistics;
#endif

        /// <summary>
        /// Get the names of statistics specific to this driver. The result is an Arrow dataset.
        /// </summary>
#if NET5_0_OR_GREATER
        internal delegate* unmanaged<CAdbcConnection*, CArrowArrayStream*, CAdbcError*, AdbcStatusCode> ConnectionGetStatisticNames;
#else
        internal IntPtr ConnectionGetStatisticNames;
#endif

        /// <summary>
        /// Set a byte* option on a connection.
        ///
        /// Options may be set before AdbcConnectionInit.  Some drivers may support setting options after initialization as well.
        /// </summary>
#if NET5_0_OR_GREATER
        internal delegate* unmanaged<CAdbcConnection*, byte*, byte*, nint, CAdbcError*, AdbcStatusCode> ConnectionSetOptionBytes;
#else
        internal IntPtr ConnectionSetOptionBytes;
#endif

        /// <summary>
        /// Set a double option on a connection.
        ///
        /// Options may be set before AdbcConnectionInit.  Some drivers may support setting options after initialization as well.
        /// </summary>
#if NET5_0_OR_GREATER
        internal delegate* unmanaged<CAdbcConnection*, byte*, double, CAdbcError*, AdbcStatusCode> ConnectionSetOptionDouble;
#else
        internal IntPtr ConnectionSetOptionDouble;
#endif

        /// <summary>
        /// Set an integer option on a connection.
        ///
        /// Options may be set before AdbcConnectionInit.  Some drivers may support setting options after initialization as well.
        /// </summary>
#if NET5_0_OR_GREATER
        internal delegate* unmanaged<CAdbcConnection*, byte*, long, CAdbcError*, AdbcStatusCode> ConnectionSetOptionInt;
#else
        internal IntPtr ConnectionSetOptionInt;
#endif

        /// <summary>
        /// Cancel execution of an in-progress query.
        /// </summary>
#if NET5_0_OR_GREATER
        internal delegate* unmanaged<CAdbcStatement*, CAdbcError*, AdbcStatusCode> StatementCancel;
#else
        internal IntPtr StatementCancel;
#endif

        /// <summary>
        /// Get the schema of the result set of a query without executing it.
        /// </summary>
#if NET5_0_OR_GREATER
        internal delegate* unmanaged<CAdbcStatement*, CArrowSchema*, CAdbcError*, AdbcStatusCode> StatementExecuteSchema;
#else
        internal IntPtr StatementExecuteSchema;
#endif

        /// <summary>
        /// Get a string option of the statement.
        /// </summary>
#if NET5_0_OR_GREATER
        internal delegate* unmanaged<CAdbcStatement*, byte*, byte*, nint*, CAdbcError*, AdbcStatusCode> StatementGetOption;
#else
        internal IntPtr StatementGetOption;
#endif

        /// <summary>
        /// Get a byte* option of the statement.
        /// </summary>
#if NET5_0_OR_GREATER
        internal delegate* unmanaged<CAdbcStatement*, byte*, byte*, nint*, CAdbcError*, AdbcStatusCode> StatementGetOptionBytes;
#else
        internal IntPtr StatementGetOptionBytes;
#endif

        /// <summary>
        /// Get a double option of the statement.
        /// </summary>
#if NET5_0_OR_GREATER
        internal delegate* unmanaged<CAdbcStatement*, byte*, double*, CAdbcError*, AdbcStatusCode> StatementGetOptionDouble;
#else
        internal IntPtr StatementGetOptionDouble;
#endif

        /// <summary>
        /// Get an integer option of the statement.
        /// </summary>
#if NET5_0_OR_GREATER
        internal delegate* unmanaged<CAdbcStatement*, byte*, long*, CAdbcError*, AdbcStatusCode> StatementGetOptionInt;
#else
        internal IntPtr StatementGetOptionInt;
#endif

        /// <summary>
        /// Set a byte* option on a statement.
        /// </summary>
#if NET5_0_OR_GREATER
        internal delegate* unmanaged<CAdbcStatement*, byte*, byte*, nint, CAdbcError*, AdbcStatusCode> StatementSetOptionBytes;
#else
        internal IntPtr StatementSetOptionBytes;
#endif

        /// <summary>
        /// Set a double option on a statement.
        /// </summary>
#if NET5_0_OR_GREATER
        internal delegate* unmanaged<CAdbcStatement*, byte*, double, CAdbcError*, AdbcStatusCode> StatementSetOptionDouble;
#else
        internal IntPtr StatementSetOptionDouble;
#endif

        /// <summary>
        /// Set an integer option on a statement.
        /// </summary>
#if NET5_0_OR_GREATER
        internal delegate* unmanaged<CAdbcStatement*, byte*, long, CAdbcError*, AdbcStatusCode> StatementSetOptionInt;
#else
        internal IntPtr StatementSetOptionInt;
#endif

        #endregion
    }
}
