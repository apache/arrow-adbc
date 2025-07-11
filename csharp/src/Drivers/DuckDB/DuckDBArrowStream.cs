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
using Apache.Arrow.Memory;
using DuckDB.NET.Data;

namespace Apache.Arrow.Adbc.Drivers.DuckDB
{
    /// <summary>
    /// Converts DuckDB query results to Arrow format.
    /// </summary>
    public class DuckDBArrowStream : IArrowArrayStream
    {
        private readonly DuckDBDataReader _reader;
        private readonly Schema _schema;
        private readonly int _batchSize;
        private readonly ArrowTypeConverter _converter;
        private bool _disposed;

        public DuckDBArrowStream(DuckDBDataReader reader, Schema schema, int batchSize = 1024)
        {
            _reader = reader ?? throw new ArgumentNullException(nameof(reader));
            _schema = schema ?? throw new ArgumentNullException(nameof(schema));
            _batchSize = batchSize > 0 ? batchSize : throw new ArgumentOutOfRangeException(nameof(batchSize));
            _converter = new ArrowTypeConverter();
        }

        public Schema Schema => _schema;

        public async ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(nameof(DuckDBArrowStream));
            }

            // Read a batch of rows
            var values = new List<object?[]>();
            var rowCount = 0;

            while (rowCount < _batchSize && await _reader.ReadAsync(cancellationToken))
            {
                var row = new object?[_reader.FieldCount];
                for (int i = 0; i < _reader.FieldCount; i++)
                {
                    row[i] = _reader.IsDBNull(i) ? null : _reader.GetValue(i);
                }
                values.Add(row);
                rowCount++;
            }

            if (rowCount == 0)
            {
                return null;
            }

            // Convert to Arrow arrays
            var arrays = new IArrowArray[_schema.FieldsList.Count];
            
            for (int columnIndex = 0; columnIndex < _schema.FieldsList.Count; columnIndex++)
            {
                var field = _schema.FieldsList[columnIndex];
                var columnValues = new List<object?>(rowCount);
                
                for (int rowIndex = 0; rowIndex < rowCount; rowIndex++)
                {
                    columnValues.Add(values[rowIndex][columnIndex]);
                }
                
                arrays[columnIndex] = _converter.ConvertToArrowArray(columnValues, field.DataType);
            }

            return new RecordBatch(_schema, arrays, rowCount);
        }

        public void Dispose()
        {
            if (!_disposed)
            {
                _reader.Dispose();
                _disposed = true;
            }
        }
    }
}