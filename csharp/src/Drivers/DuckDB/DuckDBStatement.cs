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
using DuckDB.NET.Data;
using System.Data;

namespace Apache.Arrow.Adbc.Drivers.DuckDB
{
    /// <summary>
    /// DuckDB ADBC statement implementation.
    /// </summary>
    public class DuckDBStatement : AdbcStatement
    {
        private readonly DuckDBConnection _connection;
        private DuckDBCommand? _command;
        private readonly Dictionary<string, object?> _parameters;
        private int _batchSize = 1024;
        private RecordBatch? _boundBatch;
        private Schema? _boundSchema;
        private IArrowArrayStream? _boundStream;

        public DuckDBStatement(DuckDBConnection connection)
        {
            _connection = connection ?? throw new ArgumentNullException(nameof(connection));
            _parameters = new Dictionary<string, object?>();
        }

        public override void Bind(RecordBatch batch, Schema? schema)
        {
            _boundBatch = batch ?? throw new ArgumentNullException(nameof(batch));
            _boundSchema = schema ?? batch.Schema;
            _boundStream = null; // Clear any previously bound stream
        }

        public override void BindStream(IArrowArrayStream stream)
        {
            _boundStream = stream ?? throw new ArgumentNullException(nameof(stream));
            _boundBatch = null; // Clear any previously bound batch
            _boundSchema = null;
        }

        public override UpdateResult ExecuteUpdate()
        {
            ValidateStatement();
            
            // If we have bound parameters, execute for each row
            if (_boundBatch != null || _boundStream != null)
            {
                return ExecuteUpdateWithBoundParameters();
            }
            
            // Always reset command for updates to ensure we have the correct SQL
            ResetCommand();
            var command = GetCommand();
            var affectedRows = command.ExecuteNonQuery();
            
            // DuckDB returns 0 for DDL operations, but ADBC expects -1
            if (affectedRows == 0 && IsDdlStatement(SqlQuery))
            {
                affectedRows = -1;
            }
            
            return new UpdateResult(affectedRows);
        }

        public override async Task<UpdateResult> ExecuteUpdateAsync()
        {
            ValidateStatement();
            
            // If we have bound parameters, execute for each row
            if (_boundBatch != null || _boundStream != null)
            {
                return await ExecuteUpdateWithBoundParametersAsync();
            }
            
            // Always reset command for updates to ensure we have the correct SQL
            ResetCommand();
            var command = GetCommand();
            var affectedRows = await command.ExecuteNonQueryAsync();
            
            // DuckDB returns 0 for DDL operations, but ADBC expects -1
            if (affectedRows == 0 && IsDdlStatement(SqlQuery))
            {
                affectedRows = -1;
            }
            
            return new UpdateResult(affectedRows);
        }

        public override QueryResult ExecuteQuery()
        {
            ValidateStatement();
            
            // Always reset command for queries to ensure we have the correct SQL
            ResetCommand();
            var command = GetCommand();
            var reader = command.ExecuteReader();
            
            try
            {
                var duckDbReader = (DuckDBDataReader)reader;
                var schema = DuckDBSchemaConverter.ConvertToArrowSchema(duckDbReader);
                var stream = new DuckDBArrowStream(duckDbReader, schema, _batchSize);
                
                // Get affected rows if available
                long affectedRows = reader.RecordsAffected;
                
                return new QueryResult(affectedRows, stream);
            }
            catch
            {
                reader.Dispose();
                throw;
            }
        }

        public override async ValueTask<QueryResult> ExecuteQueryAsync()
        {
            ValidateStatement();
            
            // Always reset command for queries to ensure we have the correct SQL
            ResetCommand();
            var command = GetCommand();
            var reader = await command.ExecuteReaderAsync();
            
            try
            {
                var duckDbReader = (DuckDBDataReader)reader;
                var schema = DuckDBSchemaConverter.ConvertToArrowSchema(duckDbReader);
                var stream = new DuckDBArrowStream(duckDbReader, schema, _batchSize);
                
                // Get affected rows if available
                long affectedRows = reader.RecordsAffected;
                
                return new QueryResult(affectedRows, stream);
            }
            catch
            {
                reader.Dispose();
                throw;
            }
        }

        // No need to override SetSqlQuery, just use the property

        public override void SetOption(string key, string value)
        {
            switch (key)
            {
                case DuckDBParameters.BatchSize:
                    if (int.TryParse(value, out var batchSize) && batchSize > 0)
                    {
                        _batchSize = batchSize;
                    }
                    else
                    {
                        throw new ArgumentException($"Invalid batch size: {value}");
                    }
                    break;
                    
                default:
                    base.SetOption(key, value);
                    break;
            }
        }

        public override void Prepare()
        {
            ValidateStatement();
            var command = GetCommand();
            command.Prepare();
        }

        public override Schema GetParameterSchema()
        {
            // DuckDB doesn't provide parameter schema information
            throw new NotSupportedException("DuckDB does not support parameter schema retrieval");
        }

        // Parameter binding not implemented yet
        // TODO: Add parameter support

        public override void Dispose()
        {
            _command?.Dispose();
            _command = null;
            base.Dispose();
        }

        private void ValidateStatement()
        {
            if (string.IsNullOrEmpty(SqlQuery))
            {
                throw new InvalidOperationException("SQL query is not set");
            }
        }

        private DuckDBCommand GetCommand()
        {
            if (_command == null)
            {
                var connection = _connection.GetConnection();
                _command = connection.CreateCommand();
                _command.CommandText = SqlQuery ?? string.Empty;
                
                // Add parameters
                foreach (var kvp in _parameters)
                {
                    var parameter = _command.CreateParameter();
                    parameter.ParameterName = kvp.Key;
                    parameter.Value = kvp.Value ?? DBNull.Value;
                    _command.Parameters.Add(parameter);
                }
            }
            
            return _command;
        }

        private void ResetCommand()
        {
            _command?.Dispose();
            _command = null;
        }

        private static bool IsDdlStatement(string? sql)
        {
            if (string.IsNullOrWhiteSpace(sql))
                return false;
                
            var trimmedSql = sql.TrimStart().ToUpperInvariant();
            return trimmedSql.StartsWith("CREATE ") ||
                   trimmedSql.StartsWith("DROP ") ||
                   trimmedSql.StartsWith("ALTER ") ||
                   trimmedSql.StartsWith("TRUNCATE ") ||
                   trimmedSql.StartsWith("RENAME ") ||
                   trimmedSql.StartsWith("COMMENT ");
        }

        private UpdateResult ExecuteUpdateWithBoundParameters()
        {
            long totalAffectedRows = 0;

            if (_boundBatch != null)
            {
                // Execute for each row in the bound batch
                for (int i = 0; i < _boundBatch.Length; i++)
                {
                    ResetCommand(); // Create fresh command for each row
                    var command = GetCommand();
                    
                    // Bind parameters for this row
                    BindParametersFromArrow(_boundBatch, i, command);
                    
                    totalAffectedRows += command.ExecuteNonQuery();
                }
            }
            else if (_boundStream != null)
            {
                // Execute for each batch in the stream
                RecordBatch? batch;
                while ((batch = _boundStream.ReadNextRecordBatchAsync().Result) != null)
                {
                    for (int i = 0; i < batch.Length; i++)
                    {
                        ResetCommand(); // Create fresh command for each row
                        var command = GetCommand();
                        
                        // Bind parameters for this row
                        BindParametersFromArrow(batch, i, command);
                        
                        totalAffectedRows += command.ExecuteNonQuery();
                    }
                    batch.Dispose();
                }
            }

            return new UpdateResult(totalAffectedRows);
        }

        private async Task<UpdateResult> ExecuteUpdateWithBoundParametersAsync()
        {
            long totalAffectedRows = 0;

            if (_boundBatch != null)
            {
                // Execute for each row in the bound batch
                for (int i = 0; i < _boundBatch.Length; i++)
                {
                    ResetCommand(); // Create fresh command for each row
                    var command = GetCommand();
                    
                    // Bind parameters for this row
                    BindParametersFromArrow(_boundBatch, i, command);
                    
                    try
                    {
                        var affectedRows = await command.ExecuteNonQueryAsync();
                        totalAffectedRows += affectedRows;
                    }
                    catch (Exception ex)
                    {
                        var paramInfo = string.Join(", ", command.Parameters.Cast<DuckDBParameter>().Select(p => $"{p.ParameterName ?? "?"}: {p.Value}"));
                        throw new InvalidOperationException($"Failed to execute bound statement for row {i}. SQL: {SqlQuery}, Parameters: {paramInfo}", ex);
                    }
                }
            }
            else if (_boundStream != null)
            {
                // Execute for each batch in the stream
                RecordBatch? batch;
                while ((batch = await _boundStream.ReadNextRecordBatchAsync()) != null)
                {
                    for (int i = 0; i < batch.Length; i++)
                    {
                        ResetCommand(); // Create fresh command for each row
                        var command = GetCommand();
                        
                        // Bind parameters for this row
                        BindParametersFromArrow(batch, i, command);
                        
                        try
                        {
                            var affectedRows = await command.ExecuteNonQueryAsync();
                            totalAffectedRows += affectedRows;
                        }
                        catch (Exception ex)
                        {
                            var paramInfo = string.Join(", ", command.Parameters.Cast<DuckDBParameter>().Select(p => $"{p.ParameterName ?? "?"}: {p.Value}"));
                            throw new InvalidOperationException($"Failed to execute bound statement for batch row {i}. SQL: {SqlQuery}, Parameters: {paramInfo}", ex);
                        }
                    }
                    batch.Dispose();
                }
            }

            return new UpdateResult(totalAffectedRows);
        }

        private void BindParametersFromArrow(RecordBatch batch, int rowIndex, DuckDBCommand command)
        {
            command.Parameters.Clear();

            // Validate parameter count matches SQL placeholders
            var sqlPlaceholders = SqlQuery?.Count(c => c == '?') ?? 0;
            if (sqlPlaceholders != batch.ColumnCount)
            {
                throw new InvalidOperationException($"SQL query has {sqlPlaceholders} parameter placeholders but RecordBatch has {batch.ColumnCount} columns");
            }

            for (int colIndex = 0; colIndex < batch.ColumnCount; colIndex++)
            {
                var column = batch.Column(colIndex);
                var parameter = command.CreateParameter();
                
                // DuckDB uses positional parameters, not named ones
                // Don't set ParameterName - let DuckDB handle positional mapping
                
                // Get value from Arrow array
                try
                {
                    var value = GetValueFromArrowArray(column, rowIndex);
                    parameter.Value = value ?? DBNull.Value;
                    
                    // Debug: Log null values
                    if (value == null)
                    {
                        var field = _boundSchema?.GetFieldByIndex(colIndex) ?? batch.Schema.GetFieldByIndex(colIndex);
                        System.Diagnostics.Debug.WriteLine($"Setting NULL for column {colIndex} ({field.Name}), row {rowIndex}");
                    }
                }
                catch (Exception ex)
                {
                    var field = _boundSchema?.GetFieldByIndex(colIndex) ?? batch.Schema.GetFieldByIndex(colIndex);
                    throw new InvalidOperationException($"Failed to convert value from column {colIndex} ({field.Name}): {ex.Message}", ex);
                }
                
                command.Parameters.Add(parameter);
            }
        }

        private object? GetValueFromArrowArray(IArrowArray array, int index)
        {
            if (array.IsNull(index))
            {
                return null;
            }

            switch (array)
            {
                case BooleanArray boolArray:
                    return boolArray.GetValue(index);
                    
                case Int8Array int8Array:
                    return int8Array.GetValue(index);
                    
                case Int16Array int16Array:
                    return int16Array.GetValue(index);
                    
                case Int32Array int32Array:
                    return int32Array.GetValue(index);
                    
                case Int64Array int64Array:
                    return int64Array.GetValue(index);
                    
                case UInt8Array uint8Array:
                    return uint8Array.GetValue(index);
                    
                case UInt16Array uint16Array:
                    return uint16Array.GetValue(index);
                    
                case UInt32Array uint32Array:
                    return uint32Array.GetValue(index);
                    
                case UInt64Array uint64Array:
                    var uint64Value = uint64Array.GetValue(index);
                    return uint64Value.HasValue ? (long)uint64Value.Value : null; // DuckDB doesn't support uint64, convert to int64
                    
                case FloatArray floatArray:
                    return floatArray.GetValue(index);
                    
                case DoubleArray doubleArray:
                    return doubleArray.GetValue(index);
                    
                case StringArray stringArray:
                    return stringArray.GetString(index);
                    
                case BinaryArray binaryArray:
                    return binaryArray.GetBytes(index).ToArray();
                    
                case Date32Array date32Array:
                    return date32Array.GetDateTime(index);
                    
                case Date64Array date64Array:
                    return date64Array.GetDateTime(index);
                    
                case TimestampArray timestampArray:
                    return timestampArray.GetTimestamp(index).GetValueOrDefault();
                    
                case Time32Array time32Array:
                    var time32Value = time32Array.GetValue(index);
                    return time32Value.HasValue ? new TimeSpan(0, 0, 0, 0, time32Value.Value) : null;
                    
                case Time64Array time64Array:
                    var time64Value = time64Array.GetValue(index);
                    return time64Value.HasValue ? new TimeSpan(time64Value.Value * 100) : null; // nanoseconds to ticks
                    
                case Decimal128Array decimal128Array:
                    return decimal128Array.GetValue(index);
                    
                case Decimal256Array decimal256Array:
                    return decimal256Array.GetValue(index);
                    
                default:
                    throw new NotSupportedException($"Arrow type {array.GetType().Name} is not supported for parameter binding");
            }
        }
    }
}