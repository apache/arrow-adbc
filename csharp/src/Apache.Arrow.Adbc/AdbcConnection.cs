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
using System.Collections.Generic;
using Apache.Arrow.Ipc;

namespace Apache.Arrow.Adbc
{
    /// <summary>
    /// Provides methods for query execution, managing prepared statements,
    /// using transactions, and so on.
    /// </summary>
    public abstract class AdbcConnection : IDisposable
    {
        private bool _autoCommit = true;
        private bool _readOnly = false;
        private IsolationLevel _isolationLevel = IsolationLevel.Default;

        /// <summary>
        /// Commit the pending transaction.
        /// </summary>
        public virtual void Commit()
        {
            throw AdbcException.NotImplemented("Connection does not support transactions");
        }

        /// <summary>
        /// Create a new statement that can be executed.
        /// </summary>
        public abstract AdbcStatement CreateStatement();

        /// <summary>
        /// Create a new statement to bulk insert into a table.
        /// </summary>
        /// <param name="targetTableName">The table name</param>
        /// <param name="mode">The ingest mode</param>
        public virtual AdbcStatement BulkIngest(string targetTableName, BulkIngestMode mode)
        {
            throw AdbcException.NotImplemented("Connection does not support BulkIngest");
        }

        /// <summary>
        /// Create a new statement to bulk insert into a table.
        /// </summary>
        /// <param name="targetCatalog">The catalog name, or null to use the current catalog</param>
        /// <param name="targetDbSchema">The schema name, or null to use the current schema</param>
        /// <param name="targetTableName">The table name</param>
        /// <param name="mode">The ingest mode</param>
        /// <param name="isTemporary">True for a temporary table. Catalog and Schema must be null when true.</param>
        public virtual AdbcStatement BulkIngest(string? targetCatalog, string? targetDbSchema, string targetTableName, BulkIngestMode mode, bool isTemporary)
        {
            throw AdbcException.NotImplemented("Connection does not support BulkIngest");
        }

        public virtual void Dispose()
        {
        }

        /// <summary>
        /// Get metadata about the driver/database.
        /// </summary>
        /// <param name="codes">
        /// The metadata items to fetch.
        /// </param>
        /// <returns>
        /// Metadata about the driver and/or database
        /// </returns>
        /// <exception cref="ArgumentNullException"></exception>
        public virtual IArrowArrayStream GetInfo(IReadOnlyList<AdbcInfoCode> codes)
        {
            if (codes == null)
                throw new ArgumentNullException(nameof(codes));

            throw AdbcException.NotImplemented("Connection does not support GetInfo");
        }

        /// <summary>
        /// Get a hierarchical view of all catalogs, database schemas, tables,
        /// and columns.
        /// </summary>
        /// <param name="depth">
        /// The level of nesting to display.
        /// If ALL, display all levels (up through columns).
        /// If CATALOGS, display only catalogs (i.e., catalog_schemas will be
        /// null), and so on. May be a* search pattern.
        /// </param>
        /// <param name="catalogPattern">
        /// Only show tables in the given catalog.
        /// If null, do not filter by catalog.If an empty string, only show tables
        /// without a catalog. May be a search pattern.
        /// </param>
        /// <param name="dbSchemaPattern">
        /// Only show tables in the given database schema. If null, do not
        /// filter by database schema.If an empty string, only show tables
        /// without a database schema. May be a search pattern.
        /// </param>
        /// <param name="tableNamePattern">
        /// Only show tables with the given name. If an empty string, only
        /// show tables without a catalog. May be a search pattern.
        /// </param>
        /// <param name="tableTypes">
        /// Only show tables matching one of the given table types.
        /// If null, show tables of any type. Valid table types can be
        /// fetched from <see cref="GetTableTypes"/>}.
        /// </param>
        /// <param name="columnNamePattern">
        /// Only show columns with the given name.
        /// If null, do not filter by name.May be a search pattern.
        /// </param>
        public abstract IArrowArrayStream GetObjects(
            GetObjectsDepth depth,
            string? catalogPattern,
            string? dbSchemaPattern,
            string? tableNamePattern,
            IReadOnlyList<string>? tableTypes,
            string? columnNamePattern);

        public enum GetObjectsDepth
        {
            /// <summary>
            /// Display ALL objects (catalog, database schemas, tables,
            /// and columns).
            /// </summary>
            All,

            /// <summary>
            /// Display only catalogs.
            /// </summary>
            Catalogs,

            /// <summary>
            /// Display catalogs and database schemas.
            /// </summary>
            DbSchemas,

            /// <summary>
            /// Display catalogs, database schemas, and tables.
            /// </summary>
            Tables
        }

        /// <summary>
        /// Get the Arrow schema of a database table.
        /// </summary>
        /// <param name="catalog">
        /// The catalog of the table (or null).
        /// </param>
        /// <param name="dbSchema">
        /// The database schema of the table (or null).
        /// </param>
        /// <param name="tableName">
        /// The table name.
        /// </param>
        public abstract Schema GetTableSchema(string? catalog, string? dbSchema, string tableName);

        /// <summary>
        /// Get a list of table types supported by the database.
        /// </summary>
        public abstract IArrowArrayStream GetTableTypes();

        /// <summary>
        /// Options may be set before AdbcConnectionInit.  Some drivers may
        /// support setting options after initialization as well.
        /// </summary>
        /// <param name="key">Option name</param>
        /// <param name="value">Option value</param>
        public virtual void SetOption(string key, string value)
        {
            throw AdbcException.NotImplemented("Connection does not support setting options");
        }

        /// <summary>
        /// Create a result set from a serialized PartitionDescriptor.
        /// </summary>
        /// <param name="partition">
        /// The partition descriptor.
        /// </param>
        public virtual IArrowArrayStream ReadPartition(PartitionDescriptor partition)
        {
            throw AdbcException.NotImplemented("Connection does not support partitions");
        }

        /// <summary>
        /// Rollback the pending transaction.
        /// </summary>
        public virtual void Rollback()
        {
            throw AdbcException.NotImplemented("Connection does not support transactions");
        }

        /// <summary>
        /// Gets or sets the autocommit state.
        /// </summary>
        public virtual bool AutoCommit
        {
            get => _autoCommit;
            set => throw AdbcException.NotImplemented("Connection does not support transactions");
        }

        /// <summary>
        /// Gets or sets whether the connection is read-only.
        /// </summary>
        public virtual bool ReadOnly
        {
            get => _readOnly;
            set => throw AdbcException.NotImplemented("Connection does not support read-only mode");
        }

        /// <summary>
        /// Gets or sets the isolation level used by transactions.
        /// </summary>
        public virtual IsolationLevel IsolationLevel
        {
            get => _isolationLevel;
            set => throw AdbcException.NotImplemented("Connection does not support setting isolation level");
        }

        /// <summary>
        /// Attempts to cancel an in-progress operation on a connection.
        /// </summary>
        /// <remarks>
        /// This can be called during a method like GetObjects or while consuming an ArrowArrayStream
        /// returned from such. Calling this function should make the other function throw a cancellation exception.
        ///
        /// This must always be thread-safe.
        /// </remarks>
        public virtual void Cancel()
        {
            throw AdbcException.NotImplemented("Connection does not support cancellation");
        }

        /// <summary>
        /// Gets the names of statistics specific to this driver.
        /// </summary>
        /// <returns></returns>
        public virtual IArrowArrayStream GetStatisticsNames()
        {
            throw AdbcException.NotImplemented("Connection does not support statistics");
        }

        /// <summary>
        /// Gets statistics about the data distribution of table(s)
        /// </summary>
        /// <param name="catalogPattern">The catalog or null. May be a search pattern.</param>
        /// <param name="schemaPattern">The schema or null. May be a search pattern.</param>
        /// <param name="tableName">The table name or null. May be a search pattern.</param>
        /// <param name="approximate">If false, consumer desires exact statistics regardless of cost</param>
        /// <returns>A result describing the statistics for the table(s)</returns>
        public virtual IArrowArrayStream GetStatistics(string? catalogPattern, string? schemaPattern, string tableNamePattern, bool approximate)
        {
            throw AdbcException.NotImplemented("Connection does not support statistics");
        }
    }
}
