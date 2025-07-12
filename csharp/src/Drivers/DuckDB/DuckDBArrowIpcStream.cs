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
using System.Data;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using DuckDB.NET.Data;

namespace Apache.Arrow.Adbc.Drivers.DuckDB
{
    /// <summary>
    /// Arrow stream implementation that uses DuckDB's Arrow IPC support for better performance.
    /// </summary>
    public class DuckDBArrowIpcStream : IArrowArrayStream
    {
        private readonly DuckDBConnection _connection;
        private readonly string _query;
        private ArrowStreamReader? _reader;
        private MemoryStream? _ipcStream;
        private bool _disposed;

        /// <summary>
        /// Initializes a new instance of the <see cref="DuckDBArrowIpcStream"/> class.
        /// </summary>
        /// <param name="connection">The DuckDB connection.</param>
        /// <param name="query">The SQL query to execute.</param>
        public DuckDBArrowIpcStream(DuckDBConnection connection, string query)
        {
            _connection = connection ?? throw new ArgumentNullException(nameof(connection));
            _query = query ?? throw new ArgumentNullException(nameof(query));
        }

        /// <summary>
        /// Gets the Arrow schema for this stream.
        /// </summary>
        public Schema Schema
        {
            get
            {
                EnsureInitialized();
                return _reader!.Schema;
            }
        }

        /// <summary>
        /// Reads the next batch of records from the stream.
        /// </summary>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>A record batch, or null if no more records are available.</returns>
        public async ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
        {
            EnsureInitialized();
            
            return await _reader!.ReadNextRecordBatchAsync(cancellationToken);
        }

        private void EnsureInitialized()
        {
            if (_reader != null)
                return;

            try
            {
                // First, ensure the nanoarrow extension is loaded
                using (var loadCmd = _connection.GetConnection().CreateCommand())
                {
                    loadCmd.CommandText = "LOAD nanoarrow";
                    try
                    {
                        loadCmd.ExecuteNonQuery();
                    }
                    catch (Exception ex)
                    {
                        // Extension might already be loaded or not available
                        // Fall back to regular query if to_arrow_ipc fails
                        throw new NotSupportedException("Arrow IPC support requires the nanoarrow extension. " +
                            "Install it with: INSTALL nanoarrow FROM community; LOAD nanoarrow;", ex);
                    }
                }

                // Execute query wrapped with to_arrow_ipc()
                using var command = _connection.GetConnection().CreateCommand();
                command.CommandText = $"SELECT ipc FROM to_arrow_ipc(({_query})) WHERE NOT header";
                
                var ipcData = new List<byte[]>();
                
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        if (!reader.IsDBNull(0))
                        {
                            var value = reader.GetValue(0);
                            if (value is byte[] bytes)
                            {
                                ipcData.Add(bytes);
                            }
                            else if (value is Stream stream)
                            {
                                using var ms = new MemoryStream();
                                stream.CopyTo(ms);
                                ipcData.Add(ms.ToArray());
                            }
                        }
                    }
                }

                // Get the schema message
                command.CommandText = $"SELECT ipc FROM to_arrow_ipc(({_query})) WHERE header";
                byte[]? schemaData = null;
                
                using (var reader = command.ExecuteReader())
                {
                    if (reader.Read() && !reader.IsDBNull(0))
                    {
                        var value = reader.GetValue(0);
                        if (value is byte[] bytes)
                        {
                            schemaData = bytes;
                        }
                        else if (value is Stream stream)
                        {
                            using var ms = new MemoryStream();
                            stream.CopyTo(ms);
                            schemaData = ms.ToArray();
                        }
                    }
                }

                if (schemaData == null || ipcData.Count == 0)
                {
                    throw new InvalidOperationException("Failed to get Arrow IPC data from query");
                }

                // Combine schema and data into a single stream
                var totalSize = schemaData.Length;
                foreach (var data in ipcData)
                {
                    totalSize += data.Length;
                }

                _ipcStream = new MemoryStream(totalSize);
                _ipcStream.Write(schemaData, 0, schemaData.Length);
                foreach (var data in ipcData)
                {
                    _ipcStream.Write(data, 0, data.Length);
                }
                _ipcStream.Position = 0;

                // Create Arrow stream reader
                _reader = new ArrowStreamReader(_ipcStream);
            }
            catch (Exception ex) when (ex.Message.Contains("nanoarrow") || ex.Message.Contains("to_arrow_ipc"))
            {
                // If Arrow IPC is not available, we could fall back to the regular approach
                throw new NotSupportedException("Arrow IPC support is not available. " +
                    "Please ensure the nanoarrow extension is installed and loaded.", ex);
            }
        }

        /// <summary>
        /// Disposes of the stream and its underlying resources.
        /// </summary>
        public void Dispose()
        {
            if (!_disposed)
            {
                _reader?.Dispose();
                _ipcStream?.Dispose();
                _disposed = true;
            }
        }
    }
}