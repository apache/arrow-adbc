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
using System.Data.SqlTypes;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Apache.Spark;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Apache.Hive.Service.Rpc.Thrift;

namespace Apache.Arrow.Adbc.Drivers.Apache.Hive2
{
    internal class HiveServer2Reader : IArrowArrayStream
    {
        private HiveServer2Statement? _statement;
        private readonly long _batchSize;
        private readonly HiveServer2DataTypeConversion _dataTypeConversion;
        private static readonly IReadOnlyDictionary<ArrowTypeId, Func<StringArray, IArrowType, IArrowArray>> s_arrowStringConverters =
            new Dictionary<ArrowTypeId, Func<StringArray, IArrowType, IArrowArray>>()
            {
                { ArrowTypeId.Date32, ConvertToDate32 },
                { ArrowTypeId.Decimal128, ConvertToDecimal128 },
                { ArrowTypeId.Timestamp, ConvertToTimestamp },
            };


        public HiveServer2Reader(
            HiveServer2Statement statement,
            Schema schema,
            long batchSize = HiveServer2Connection.BatchSizeDefault,
            HiveServer2DataTypeConversion dataTypeConversion = HiveServer2DataTypeConversion.None)
        {
            _statement = statement;
            Schema = schema;
            _batchSize = batchSize;
            _dataTypeConversion = dataTypeConversion;
        }

        public Schema Schema { get; }

        public async ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
        {
            if (_statement == null)
            {
                return null;
            }

            var request = new TFetchResultsReq(_statement.OperationHandle, TFetchOrientation.FETCH_NEXT, _batchSize);
            TFetchResultsResp response = await _statement.Connection.Client.FetchResults(request, cancellationToken);

            int columnCount = response.Results.Columns.Count;
            IList<IArrowArray> columnData = [];
            bool shouldConvertScalar = _dataTypeConversion == HiveServer2DataTypeConversion.Scalar;
            for (int i = 0; i < columnCount; i++)
            {
                IArrowType? expectedType = shouldConvertScalar ? Schema.FieldsList[i].DataType : null;
                IArrowArray columnArray = GetArray(response.Results.Columns[i], expectedType);
                columnData.Add(columnArray);
            }

            int length = columnCount > 0 ? GetArray(response.Results.Columns[0]).Length : 0;
            var result = new RecordBatch(
                Schema,
                columnData,
                length);

            if (!response.HasMoreRows)
            {
                _statement = null;
            }

            return result;
        }

        public void Dispose()
        {
        }

        private static IArrowArray GetArray(TColumn column, IArrowType? expectedArrowType = default)
        {
            IArrowArray arrowArray =
                (IArrowArray?)column.BoolVal?.Values ??
                (IArrowArray?)column.ByteVal?.Values ??
                (IArrowArray?)column.I16Val?.Values ??
                (IArrowArray?)column.I32Val?.Values ??
                (IArrowArray?)column.I64Val?.Values ??
                (IArrowArray?)column.DoubleVal?.Values ??
                (IArrowArray?)column.StringVal?.Values ??
                (IArrowArray?)column.BinaryVal?.Values ??
                throw new InvalidOperationException("unsupported data type");
            if (expectedArrowType != null && arrowArray is StringArray stringArray && s_arrowStringConverters.ContainsKey(expectedArrowType.TypeId))
            {
                // Perform a conversion from string to native/scalar type.
                Func<StringArray, IArrowType, IArrowArray> converter = s_arrowStringConverters[expectedArrowType.TypeId];
                return converter(stringArray, expectedArrowType);
            }
            return arrowArray;
        }

        private static Date32Array ConvertToDate32(StringArray array, IArrowType _)
        {
            var resultArray = new Date32Array.Builder();
            foreach (string item in (IReadOnlyCollection<string>)array)
            {
                resultArray.Append(DateTime.Parse(item));
            }

            return resultArray.Build();
        }

        private static Decimal128Array ConvertToDecimal128(StringArray array, IArrowType schemaType)
        {
            // Using the schema type to get the precision and scale.
            var resultArray = new Decimal128Array.Builder((Decimal128Type)schemaType);
            foreach (string item in (IReadOnlyList<string>)array)
            {
                // Trying to parse the value into a decimal to handle the exponent syntax. But this might overflow.
                if (decimal.TryParse(item, NumberStyles.Float, CultureInfo.InvariantCulture, out decimal decimalValue))
                {
                    resultArray.Append(new SqlDecimal(decimalValue));
                }
                else
                {
                    resultArray.Append(item);
                }
            }
            return resultArray.Build();
        }

        private static TimestampArray ConvertToTimestamp(StringArray array, IArrowType _)
        {
            // Match the precision of the server
            var resultArrayBuiilder = new TimestampArray.Builder(TimeUnit.Microsecond);
            foreach (string item in (IReadOnlyList<string>)array)
            {
                DateTimeOffset value = DateTimeOffset.Parse(item, DateTimeFormatInfo.InvariantInfo, DateTimeStyles.AssumeUniversal);
                resultArrayBuiilder.Append(value);
            }
            return resultArrayBuiilder.Build();
        }
    }
}
