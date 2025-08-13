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
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using DuckDB.NET.Data;
using DuckDBNETConnection = DuckDB.NET.Data.DuckDBConnection;

namespace Apache.Arrow.Adbc.Drivers.DuckDB
{
    /// <summary>
    /// DuckDB ADBC connection implementation.
    /// </summary>
    public class DuckDBConnection : AdbcConnection
    {
        private DuckDBNETConnection? _connection;
        private readonly Dictionary<string, string> _properties;
        private readonly object _lock = new object();

        /// <summary>
        /// Initializes a new instance of the <see cref="DuckDBConnection"/> class.
        /// </summary>
        /// <param name="properties">The connection properties.</param>
        public DuckDBConnection(IReadOnlyDictionary<string, string> properties)
        {
            _properties = new Dictionary<string, string>();
            foreach (var kvp in properties)
            {
                _properties[kvp.Key] = kvp.Value;
            }
        }

        /// <summary>
        /// Disposes of the connection and its resources.
        /// </summary>
        public override void Dispose()
        {
            lock (_lock)
            {
                if (_connection != null)
                {
                    _connection.Dispose();
                    _connection = null;
                }
            }
            base.Dispose();
        }

        /// <summary>
        /// Creates a new statement for this connection.
        /// </summary>
        /// <returns>A new ADBC statement.</returns>
        public override AdbcStatement CreateStatement()
        {
            ValidateConnection();
            return new DuckDBStatement(this);
        }

        /// <summary>
        /// Gets the schema for a specific table.
        /// </summary>
        /// <param name="catalog">The catalog name (optional).</param>
        /// <param name="dbSchema">The schema name (optional).</param>
        /// <param name="tableName">The table name.</param>
        /// <returns>The Arrow schema for the table.</returns>
        public override Schema GetTableSchema(string? catalog, string? dbSchema, string tableName)
        {
            ValidateConnection();
            
            var query = @"
                SELECT 
                    column_name,
                    data_type,
                    is_nullable,
                    column_default,
                    character_maximum_length,
                    numeric_precision,
                    numeric_scale
                FROM information_schema.columns
                WHERE table_name = ?";

            var parameters = new List<object?> { tableName };
            
            if (!string.IsNullOrEmpty(dbSchema))
            {
                query += " AND table_schema = ?";
                parameters.Add(dbSchema);
            }

            if (!string.IsNullOrEmpty(catalog))
            {
                query += " AND table_catalog = ?";
                parameters.Add(catalog);
            }

            query += " ORDER BY ordinal_position";

            using var command = _connection!.CreateCommand();
            command.CommandText = query;
            
            foreach (var param in parameters)
            {
                var dbParam = command.CreateParameter();
                dbParam.Value = param ?? DBNull.Value;
                command.Parameters.Add(dbParam);
            }

            using var reader = command.ExecuteReader();
            var fields = new List<Field>();

            while (reader.Read())
            {
                var columnName = reader.GetString(0);
                var dataType = reader.GetString(1);
                var isNullable = reader.GetString(2) == "YES";

                var arrowType = MapDuckDBTypeToArrow(dataType);
                fields.Add(new Field(columnName, arrowType, isNullable));
            }

            return new Schema(fields, null);
        }

        /// <summary>
        /// Gets database objects matching the specified criteria.
        /// </summary>
        /// <param name="depth">The depth of information to retrieve.</param>
        /// <param name="catalogPattern">The catalog pattern to match.</param>
        /// <param name="dbSchemaPattern">The schema pattern to match.</param>
        /// <param name="tableNamePattern">The table name pattern to match.</param>
        /// <param name="tableTypes">The table types to include.</param>
        /// <param name="columnNamePattern">The column name pattern to match.</param>
        /// <returns>An Arrow stream containing the matching objects.</returns>
        public override IArrowArrayStream GetObjects(GetObjectsDepth depth, string? catalogPattern, string? dbSchemaPattern, string? tableNamePattern, IReadOnlyList<string>? tableTypes, string? columnNamePattern)
        {
            ValidateConnection();
            
            // Use simplified implementation for now to avoid complex nested structure issues
            return new GetObjectsReaderSimple();
        }

        /// <summary>
        /// Gets information about the database and driver.
        /// </summary>
        /// <param name="codes">The information codes to retrieve.</param>
        /// <returns>An Arrow stream containing the requested information.</returns>
        public override IArrowArrayStream GetInfo(IReadOnlyList<AdbcInfoCode> codes)
        {
            ValidateConnection();
            return new InfoArrowStream(_connection!, codes);
        }

        /// <summary>
        /// Gets the available table types in the database.
        /// </summary>
        /// <returns>An Arrow stream containing the table types.</returns>
        public override IArrowArrayStream GetTableTypes()
        {
            ValidateConnection();
            
            // DuckDB supports these table types
            var tableTypes = new List<string>
            {
                "BASE TABLE",
                "VIEW",
                "TEMPORARY TABLE",
                "TEMPORARY VIEW"
            };

            var schema = new Schema(new List<Field>
            {
                new Field("table_type", StringType.Default, false)
            }, null);

            var batch = new RecordBatch(schema, new IArrowArray[]
            {
                new StringArray.Builder().AppendRange(tableTypes).Build()
            }, tableTypes.Count);

            return new ListArrayStream(schema, new[] { batch });
        }

        /// <summary>
        /// Commits the current transaction.
        /// </summary>
        public override void Commit()
        {
            ValidateConnection();
            
            using var command = _connection!.CreateCommand();
            command.CommandText = "COMMIT";
            command.ExecuteNonQuery();
        }

        /// <summary>
        /// Rolls back the current transaction.
        /// </summary>
        public override void Rollback()
        {
            ValidateConnection();
            
            using var command = _connection!.CreateCommand();
            command.CommandText = "ROLLBACK";
            command.ExecuteNonQuery();
        }

        /// <summary>
        /// Gets or sets whether the connection is in auto-commit mode.
        /// </summary>
        public override bool AutoCommit
        {
            get
            {
                ValidateConnection();
                // DuckDB is auto-commit by default
                return base.AutoCommit;
            }
            set
            {
                ValidateConnection();
                
                if (!value && base.AutoCommit)
                {
                    // Turning off auto-commit, start a transaction
                    using var command = _connection!.CreateCommand();
                    command.CommandText = "BEGIN TRANSACTION";
                    command.ExecuteNonQuery();
                }
                else if (value && !base.AutoCommit)
                {
                    // Turning on auto-commit, commit any pending transaction
                    try
                    {
                        Commit();
                    }
                    catch
                    {
                        // Ignore errors if no transaction is active
                    }
                }
                
                base.AutoCommit = value;
            }
        }

        /// <summary>
        /// Gets or sets whether the connection is read-only.
        /// </summary>
        public override bool ReadOnly
        {
            get
            {
                ValidateConnection();
                // DuckDB doesn't have a read-only mode at connection level
                return base.ReadOnly;
            }
            set
            {
                if (value)
                {
                    throw new NotSupportedException("DuckDB does not support read-only connections");
                }
                base.ReadOnly = value;
            }
        }

        /// <summary>
        /// Gets or sets the transaction isolation level.
        /// </summary>
        public override IsolationLevel IsolationLevel
        {
            get
            {
                ValidateConnection();
                // DuckDB uses snapshot isolation
                return IsolationLevel.Snapshot;
            }
            set
            {
                if (value != IsolationLevel.Snapshot && value != IsolationLevel.Default)
                {
                    throw new NotSupportedException($"DuckDB only supports snapshot isolation, not {value}");
                }
                base.IsolationLevel = value;
            }
        }

        internal DuckDBNETConnection GetConnection()
        {
            ValidateConnection();
            return _connection!;
        }

        /// <summary>
        /// Validates that the connection is open and ready for use.
        /// </summary>
        protected virtual void ValidateConnection()
        {
            lock (_lock)
            {
                if (_connection == null)
                {
                    InitializeConnection();
                }
            }
        }

        private void InitializeConnection()
        {
            var connectionString = BuildConnectionString();
            _connection = new DuckDBNETConnection(connectionString);
            _connection.Open();
        }

        private string BuildConnectionString()
        {
            var parts = new List<string>();

            if (_properties.TryGetValue(DuckDBParameters.DataSource, out var dataSource))
            {
                parts.Add($"DataSource={dataSource}");
            }
            else if (_properties.TryGetValue(DuckDBParameters.Uri, out var uri))
            {
                // Extract data source from URI
                parts.Add($"DataSource={uri}");
            }
            else
            {
                // Default to in-memory database
                parts.Add("DataSource=:memory:");
            }

            // Add any other DuckDB-specific properties
            foreach (var kvp in _properties.Where(p => p.Key != DuckDBParameters.DataSource && p.Key != DuckDBParameters.Uri))
            {
                parts.Add($"{kvp.Key}={kvp.Value}");
            }

            return string.Join(";", parts);
        }

        private static IArrowType MapDuckDBTypeToArrow(string duckDbType)
        {
            var upperType = duckDbType.ToUpperInvariant();
            
            // Handle parameterized types
            if (upperType.StartsWith("DECIMAL"))
            {
                // Parse precision and scale from DECIMAL(p,s)
                return new Decimal128Type(38, 4); // Default values, should parse from type string
            }
            
            if (upperType.StartsWith("VARCHAR"))
            {
                return StringType.Default;
            }

            return upperType switch
            {
                "BOOLEAN" => BooleanType.Default,
                "TINYINT" => Int8Type.Default,
                "SMALLINT" => Int16Type.Default,
                "INTEGER" or "INT" => Int32Type.Default,
                "BIGINT" => Int64Type.Default,
                "UTINYINT" => UInt8Type.Default,
                "USMALLINT" => UInt16Type.Default,
                "UINTEGER" or "UINT" => UInt32Type.Default,
                "UBIGINT" => UInt64Type.Default,
                "FLOAT" or "REAL" => FloatType.Default,
                "DOUBLE" => DoubleType.Default,
                "DATE" => Date32Type.Default,
                "TIME" => new Time64Type(TimeUnit.Microsecond),
                "TIMESTAMP" => new TimestampType(TimeUnit.Microsecond, (string?)null),
                "TIMESTAMP WITH TIME ZONE" => new TimestampType(TimeUnit.Microsecond, "UTC"),
                "BLOB" => BinaryType.Default,
                "UUID" => new FixedSizeBinaryType(16),
                "HUGEINT" => new Decimal128Type(38, 0),
                _ => StringType.Default // Default fallback
            };
        }
    }
}