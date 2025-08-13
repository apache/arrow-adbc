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
    /// Provides database information as an Arrow stream.
    /// </summary>
    internal class InfoArrowStream : IArrowArrayStream
    {
        private readonly DuckDBNETConnection _connection;
        private readonly IReadOnlyList<AdbcInfoCode> _codes;
        private readonly Schema _schema;
        private bool _hasBeenRead;

        public InfoArrowStream(DuckDBNETConnection connection, IReadOnlyList<AdbcInfoCode> codes)
        {
            _connection = connection;
            _codes = codes;
            _schema = StandardSchemas.GetInfoSchema;
        }

        public Schema Schema => _schema;

        public ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
        {
            if (_hasBeenRead)
            {
                return new ValueTask<RecordBatch?>((RecordBatch?)null);
            }

            _hasBeenRead = true;

            var infoNameBuilder = new UInt32Array.Builder();
            var typeBuilder = new Int8Array.Builder();
            var offsetBuilder = new Int32Array.Builder();
            
            var stringValueBuilder = new StringArray.Builder();
            var boolValueBuilder = new BooleanArray.Builder();
            var int64ValueBuilder = new Int64Array.Builder();
            var int32ValueBuilder = new Int32Array.Builder();
            var stringListBuilder = new ListArray.Builder(StringType.Default);
            var mapBuilder = new MapArray.Builder(new MapType(Int32Type.Default, Int32Type.Default));

            int arrayLength = 0;
            int currentOffset = 0;

            foreach (var code in _codes)
            {
                switch (code)
                {
                    case AdbcInfoCode.DriverName:
                        infoNameBuilder.Append((uint)code);
                        typeBuilder.Append(0); // string_value
                        offsetBuilder.Append(currentOffset++);
                        stringValueBuilder.Append("DuckDB ADBC Driver");
                        arrayLength++;
                        break;

                    case AdbcInfoCode.DriverVersion:
                        infoNameBuilder.Append((uint)code);
                        typeBuilder.Append(0); // string_value
                        offsetBuilder.Append(currentOffset++);
                        stringValueBuilder.Append(GetType().Assembly.GetName().Version?.ToString() ?? "0.1.0");
                        arrayLength++;
                        break;

                    case AdbcInfoCode.DriverArrowVersion:
                        infoNameBuilder.Append((uint)code);
                        typeBuilder.Append(0); // string_value
                        offsetBuilder.Append(currentOffset++);
                        stringValueBuilder.Append("1.0.0");
                        arrayLength++;
                        break;

                    case AdbcInfoCode.DriverAdbcVersion:
                        infoNameBuilder.Append((uint)code);
                        typeBuilder.Append(0); // string_value
                        offsetBuilder.Append(currentOffset++);
                        stringValueBuilder.Append("1.0.0");
                        arrayLength++;
                        break;

                    case AdbcInfoCode.VendorName:
                        infoNameBuilder.Append((uint)code);
                        typeBuilder.Append(0); // string_value
                        offsetBuilder.Append(currentOffset++);
                        stringValueBuilder.Append("DuckDB");
                        arrayLength++;
                        break;

                    case AdbcInfoCode.VendorVersion:
                        infoNameBuilder.Append((uint)code);
                        typeBuilder.Append(0); // string_value
                        offsetBuilder.Append(currentOffset++);
                        stringValueBuilder.Append(GetDuckDBVersion());
                        arrayLength++;
                        break;

                    case AdbcInfoCode.VendorSql:
                        infoNameBuilder.Append((uint)code);
                        typeBuilder.Append(1); // bool_value
                        offsetBuilder.Append(currentOffset++);
                        boolValueBuilder.Append(true);
                        arrayLength++;
                        break;

                    case AdbcInfoCode.VendorSubstrait:
                        infoNameBuilder.Append((uint)code);
                        typeBuilder.Append(1); // bool_value
                        offsetBuilder.Append(currentOffset++);
                        boolValueBuilder.Append(false);
                        arrayLength++;
                        break;


                    default:
                        // Skip unknown codes
                        break;
                }
            }

            // Fill remaining builders with nulls to match array length
            while (stringValueBuilder.Length < arrayLength) stringValueBuilder.AppendNull();
            while (boolValueBuilder.Length < arrayLength) boolValueBuilder.AppendNull();
            while (int64ValueBuilder.Length < arrayLength) int64ValueBuilder.AppendNull();
            while (int32ValueBuilder.Length < arrayLength) int32ValueBuilder.AppendNull();

            var childrenArrays = new IArrowArray[]
            {
                stringValueBuilder.Build(),
                boolValueBuilder.Build(),
                int64ValueBuilder.Build(),
                int32ValueBuilder.Build(),
                stringListBuilder.Build(),
                mapBuilder.Build()
            };

            // Get the UnionType from the schema
            var infoValueField = _schema.GetFieldByName("info_value");
            var unionType = (UnionType)infoValueField.DataType;
            
            var typeArray = typeBuilder.Build();
            var offsetArray = offsetBuilder.Build();
            
            var infoValue = new DenseUnionArray(
                unionType,
                arrayLength,
                childrenArrays,
                typeArray.ValueBuffer,
                offsetArray.ValueBuffer,
                0 // nullCount
            );

            var batch = new RecordBatch(_schema, new IArrowArray[]
            {
                infoNameBuilder.Build(),
                infoValue
            }, arrayLength);

            return new ValueTask<RecordBatch?>(batch);
        }

        public void Dispose()
        {
            // Nothing to dispose
        }


        private string GetDuckDBVersion()
        {
            try
            {
                using var command = _connection.CreateCommand();
                command.CommandText = "SELECT version()";
                var result = command.ExecuteScalar();
                return result?.ToString() ?? "Unknown";
            }
            catch
            {
                return "Unknown";
            }
        }
    }
}