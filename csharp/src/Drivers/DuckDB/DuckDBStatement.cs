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
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Ipc;
using DuckDB.NET.Data;

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

        public DuckDBStatement(DuckDBConnection connection)
        {
            _connection = connection ?? throw new ArgumentNullException(nameof(connection));
            _parameters = new Dictionary<string, object?>();
        }

        public override void Bind(RecordBatch batch, Schema? schema)
        {
            throw new NotSupportedException("DuckDB ADBC driver does not support bulk parameter binding");
        }

        public override void BindStream(IArrowArrayStream stream)
        {
            throw new NotSupportedException("DuckDB ADBC driver does not support stream parameter binding");
        }

        public override UpdateResult ExecuteUpdate()
        {
            ValidateStatement();
            
            var command = GetCommand();
            var affectedRows = command.ExecuteNonQuery();
            
            return new UpdateResult(affectedRows);
        }

        public override async Task<UpdateResult> ExecuteUpdateAsync()
        {
            ValidateStatement();
            
            var command = GetCommand();
            var affectedRows = await command.ExecuteNonQueryAsync();
            
            return new UpdateResult(affectedRows);
        }

        public override QueryResult ExecuteQuery()
        {
            ValidateStatement();
            
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
                _command.CommandText = SqlQuery;
                
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
    }
}