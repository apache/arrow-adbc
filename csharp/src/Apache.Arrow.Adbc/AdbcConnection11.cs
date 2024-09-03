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
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Ipc;

namespace Apache.Arrow.Adbc
{
    /// <summary>
    /// Provides methods for query execution, managing prepared statements,
    /// using transactions, and so on.
    /// </summary>
    public abstract class AdbcConnection11 : IDisposable
#if NET5_0_OR_GREATER
        , IAsyncDisposable
#endif
    {
        ~AdbcConnection11() => Dispose(false);

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
        /// Starts a new transaction with the given isolation level
        /// </summary>
        /// <param name="isolationLevel">The isolation level for the new transaction.</param>
        public virtual void BeginTransaction(IsolationLevel? isolationLevel = default)
        {
            Task.Run(() => BeginTransactionAsync(isolationLevel)).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Starts a new transactionwith the given isolation level
        /// </summary>
        /// <param name="isolationLevel">The isolation level for the new transaction.</param>
        /// <param name="cancellationToken">An optional cancellation token.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        public virtual Task BeginTransactionAsync(IsolationLevel? isolationLevel = default, CancellationToken cancellationToken = default)
        {
            throw AdbcException.NotImplemented("Connection does not support transactions");
        }

        /// <summary>
        /// Create a new statement to bulk insert into a table.
        /// </summary>
        /// <param name="targetCatalog">The catalog name, or null to use the current catalog</param>
        /// <param name="targetDbSchema">The schema name, or null to use the current schema</param>
        /// <param name="targetTableName">The table name</param>
        /// <param name="mode">The ingest mode</param>
        /// <param name="isTemporary">True for a temporary table. Catalog and Schema must be null when true.</param>
        /// <returns>A statement object which can be used to bind the data to be inserted and then executed using
        /// <see cref="AdbcStatement11.ExecuteQuery" /> or <see cref="AdbcStatement11.ExecuteUpdate" /></returns>
        public virtual AdbcStatement11 BulkIngest(string? targetCatalog, string? targetDbSchema, string targetTableName, BulkIngestMode mode, bool isTemporary)
        {
            throw AdbcException.NotImplemented("Connection does not support BulkIngest");
        }

        /// <summary>
        /// Commit the pending transaction.
        /// </summary>
        public virtual void Commit()
        {
            Task.Run(() => CommitAsync()).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Commit the pending transaction.
        /// </summary>
        /// <param name="cancellationToken">An optional cancellation token.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        public virtual Task CommitAsync(CancellationToken cancellationToken = default)
        {
            throw AdbcException.NotImplemented("Connection does not support transactions");
        }

        /// <summary>
        /// Create a new statement that can be executed.
        /// </summary>
        public abstract AdbcStatement11 CreateStatement(IReadOnlyDictionary<string, object>? options = default);

        /// <summary>
        /// Get metadata about the driver/database.
        /// </summary>
        /// <param name="codes">The metadata items to fetch.</param>
        /// <returns>Metadata about the driver and/or database.</returns>
        public virtual IArrowArrayStream GetInfo(ReadOnlySpan<AdbcInfoCode> codes)
        {
            var codesArray = codes.ToArray();
            return Task.Run(() => GetInfoAsync(codesArray)).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Get metadata about the driver/database.
        /// </summary>
        /// <param name="codes">The metadata items to fetch.</param>
        /// <param name="cancellationToken">An optional cancellation token.</param>
        /// <returns>A task whose Result property contains metadata about the driver and/or database.</returns>
        public virtual Task<IArrowArrayStream> GetInfoAsync(ReadOnlySpan<AdbcInfoCode> codes, CancellationToken cancellationToken = default)
        {
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
        /// null), and so on. May be a search pattern.
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
        /// <returns>A table containing the requested information.</returns>
        public virtual IArrowArrayStream GetObjects(
            AdbcConnection.GetObjectsDepth depth,
            string? catalogPattern,
            string? dbSchemaPattern,
            string? tableNamePattern,
            IReadOnlyList<string>? tableTypes,
            string? columnNamePattern)
        {
            return Task.Run(() => GetObjectsAsync(depth, catalogPattern, dbSchemaPattern, tableNamePattern, tableTypes, columnNamePattern)).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Get a hierarchical view of all catalogs, database schemas, tables,
        /// and columns.
        /// </summary>
        /// <param name="depth">
        /// The level of nesting to display.
        /// If ALL, display all levels (up through columns).
        /// If CATALOGS, display only catalogs (i.e., catalog_schemas will be
        /// null), and so on. May be a search pattern.
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
        /// <param name="cancellationToken">An optional cancellation token.</param>
        /// <returns>A task whose Result property is a table containing the requested information.</returns>
        public virtual Task<IArrowArrayStream> GetObjectsAsync(
            AdbcConnection.GetObjectsDepth depth,
            string? catalogPattern,
            string? dbSchemaPattern,
            string? tableNamePattern,
            IReadOnlyList<string>? tableTypes,
            string? columnNamePattern,
            CancellationToken cancellationToken = default)
        {
            throw AdbcException.NotImplemented("Connection does not support GetObjects");
        }


        /// <summary>
        /// Gets an option from a statement.
        /// </summary>
        /// <param name="key">Option name</param>
        /// <returns>The option value</returns>
        public virtual object GetOption(string key)
        {
            throw AdbcException.NotImplemented("Connection does not support getting options");
        }

        /// <summary>
        /// Gets an option from a statement.
        /// </summary>
        /// <param name="key">Option name</param>
        /// <returns>The option value</returns>
        /// <param name="cancellationToken">An optional cancellation token.</param>
        /// <returns>A task whose Result property is the requested value.</returns>
        public virtual ValueTask<object> GetOptionAsync(string key, CancellationToken cancellationToken = default)
        {
            return new ValueTask<object>(GetOption(key));
        }
        /// <summary>
        /// Get the Arrow schema of a database table.
        /// </summary>
        /// <param name="catalog">The catalog of the table (or null).</param>
        /// <param name="dbSchema">The database schema of the table (or null).</param>
        /// <param name="tableName">The table name.</param>
        /// <returns>The requested table schema.</returns>
        public virtual Schema GetTableSchema(string? catalog, string? dbSchema, string tableName)
        {
            return Task.Run(() => GetTableSchemaAsync(catalog, dbSchema, tableName)).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Get the Arrow schema of a database table.
        /// </summary>
        /// <param name="catalog">The catalog of the table (or null).</param>
        /// <param name="dbSchema">The database schema of the table (or null).</param>
        /// <param name="tableName">The table name.</param>
        /// <param name="cancellationToken">An optional cancellation token.</param>
        /// <returns>A task whose Result property is the requested schema.</returns>
        public virtual Task<Schema> GetTableSchemaAsync(string? catalog, string? dbSchema, string tableName, CancellationToken cancellationToken = default)
        {
            throw AdbcException.NotImplemented("Connection does not support GetTableSchema");
        }

        /// <summary>
        /// Get a list of table types supported by the database.
        /// </summary>
        /// <returns>The list of table types.</returns>
        public virtual IArrowArrayStream GetTableTypes()
        {
            return Task.Run(() => GetTableTypesAsync()).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Get a list of table types supported by the database.
        /// </summary>
        /// <param name="cancellationToken">An optional cancellation token.</param>
        /// <returns>A task whose Result property is the list of table types.</returns>
        public virtual Task<IArrowArrayStream> GetTableTypesAsync(CancellationToken cancellationToken = default)
        {
            throw AdbcException.NotImplemented("Connection does not support GetTableTypes");
        }

        /// <summary>
        /// Options may be set before AdbcConnectionInit.  Some drivers may
        /// support setting options after initialization as well.
        /// </summary>
        /// <param name="key">Option name</param>
        /// <param name="value">Option value</param>
        public virtual void SetOption(string key, object value)
        {
            throw AdbcException.NotImplemented("Connection does not support setting options");
        }

        /// <summary>
        /// Options may be set before AdbcConnectionInit.  Some drivers may
        /// support setting options after initialization as well.
        /// </summary>
        /// <param name="key">Option name</param>
        /// <param name="value">Option value</param>
        /// <param name="cancellationToken">An optional cancellation token.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        public virtual ValueTask SetOptionAsync(string key, object value, CancellationToken cancellationToken = default)
        {
            SetOption(key, value);
            return default;
        }

        /// <summary>
        /// Create a result set from a serialized PartitionDescriptor.
        /// </summary>
        /// <param name="partition">The partition descriptor.</param>
        public virtual IArrowArrayStream ReadPartition(PartitionDescriptor partition)
        {
            return Task.Run(() => ReadPartitionAsync(partition)).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Create a result set from a serialized PartitionDescriptor.
        /// </summary>
        /// <param name="partition">The partition descriptor.</param>
        /// <param name="cancellationToken">An optional cancellation token.</param>
        /// <returns>A task whose Result property is the requested data.</returns>
        public virtual Task<IArrowArrayStream> ReadPartitionAsync(PartitionDescriptor partition, CancellationToken cancellationToken = default)
        {
            throw AdbcException.NotImplemented("Connection does not support partitions");
        }

        /// <summary>
        /// Rollback the pending transaction.
        /// </summary>
        public virtual void Rollback()
        {
            Task.Run(() => RollbackAsync()).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Rollback the pending transaction.
        /// </summary>
        /// <param name="cancellationToken">An optional cancellation token.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        public virtual Task RollbackAsync(CancellationToken cancellationToken = default)
        {
            throw AdbcException.NotImplemented("Connection does not support transactions");
        }

        /// <summary>
        /// Gets the names of the statistics returned by this driver.
        /// </summary>
        /// <returns>The names of the statistcs.</returns>
        public virtual IArrowArrayStream GetStatisticsNames()
        {
            throw AdbcException.NotImplemented("Connection does not support statistics");
        }

        /// <summary>
        /// Gets the names of the statistics returned by this driver.
        /// </summary>
        /// <param name="cancellationToken">An optional cancellation token.</param>
        /// <returns>A task whose Result property is the names of the statistics.</returns>
        public virtual ValueTask<IArrowArrayStream> GetStatisticsNamesAsync(CancellationToken cancellationToken = default)
        {
            return new ValueTask<IArrowArrayStream>(GetStatisticsNames());
        }

        /// <summary>
        /// Gets statistics about the data distribution of table(s)
        /// </summary>
        /// <param name="catalogPattern">The catalog or null. May be a search pattern.</param>
        /// <param name="schemaPattern">The schema or null. May be a search pattern.</param>
        /// <param name="tableName">The table name or null. May be a search pattern.</param>
        /// <param name="approximate">If false, consumer desires exact statistics regardless of cost</param>
        /// <returns>A table describing the requested statistics.</returns>
        public virtual IArrowArrayStream GetStatistics(string? catalogPattern, string? schemaPattern, string tableNamePattern, bool approximate)
        {
            return Task.Run(() => GetStatisticsAsync(catalogPattern, schemaPattern, tableNamePattern, approximate)).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Gets statistics about the data distribution of table(s)
        /// </summary>
        /// <param name="catalogPattern">The catalog or null. May be a search pattern.</param>
        /// <param name="schemaPattern">The schema or null. May be a search pattern.</param>
        /// <param name="tableName">The table name or null. May be a search pattern.</param>
        /// <param name="approximate">If false, consumer desires exact statistics regardless of cost</param>
        /// <returns>A result describing the statistics for the table(s)</returns>
        /// <param name="cancellationToken">An optional cancellation token.</param>
        /// <returns>A task whose Result property is a table describing the requested statistics.</returns>
        public virtual Task<IArrowArrayStream> GetStatisticsAsync(string? catalogPattern, string? schemaPattern, string tableNamePattern, bool approximate)
        {
            throw AdbcException.NotImplemented("Connection does not support statistics");
        }

        /// <inheritdoc />
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting
        /// unmanaged resources asynchronously.
        /// </summary>
        /// <returns>A task that represents the asynchronous dispose operation.</returns>
        public ValueTask DisposeAsync()
        {
            return DisposeAsyncCore();
        }

        protected virtual ValueTask DisposeAsyncCore()
        {
            Dispose();
            return default;
        }
    }
}
