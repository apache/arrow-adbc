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
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Apache.Hive.Service.Rpc.Thrift;

namespace Apache.Arrow.Adbc.Drivers.Apache.Hive2
{
    internal class HiveServer2Reader : IArrowArrayStream
    {
        private const char AsciiZero = '0';
        private const int AsciiDigitMaxIndex = '9' - AsciiZero;
        private const char AsciiDash = '-';
        private const char AsciiSpace = ' ';
        private const char AsciiColon = ':';
        private const char AsciiPeriod = '.';

        private HiveServer2Statement? _statement;
        private readonly long _batchSize;
        private readonly DataTypeConversion _dataTypeConversion;
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
            DataTypeConversion dataTypeConversion,
            long batchSize = HiveServer2Connection.BatchSizeDefault)
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
            bool shouldConvertScalar = _dataTypeConversion.HasFlag(DataTypeConversion.Scalar);
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
                if (item == null)
                {
                    resultArray.AppendNull();
                    continue;
                }

                ReadOnlySpan<char> date = item.AsSpan();
                bool isKnownFormat = date.Length >= 8 && date[4] == AsciiDash && date[7] == AsciiDash;
                if (isKnownFormat)
                {
                    DateTime value = ConvertToDateTime(date);
                    resultArray.Append(value);
                }
                else
                {
                    resultArray.Append(DateTime.Parse(item, CultureInfo.InvariantCulture));
                }
            }

            return resultArray.Build();
        }

        private static DateTime ConvertToDateTime(ReadOnlySpan<char> date)
        {
            int year;
            int month;
            int day;
#if NETCOREAPP
            year = int.Parse(date.Slice(0, 4));
            month = int.Parse(date.Slice(5, 2));
            day = int.Parse(date.Slice(8, 2));
#else
            year = int.Parse(date.Slice(0, 4).ToString());
            month = int.Parse(date.Slice(5, 2).ToString());
            day = int.Parse(date.Slice(8, 2).ToString());
#endif
            DateTime value = new(year, month, day);
            return value;
        }

        private static Decimal128Array ConvertToDecimal128(StringArray array, IArrowType schemaType)
        {
            // Using the schema type to get the precision and scale.
            Decimal128Type decimalType = (Decimal128Type)schemaType;
            var resultArray = new Decimal128Array.Builder(decimalType);
            Span<byte> buffer = stackalloc byte[decimalType.ByteWidth];
            foreach (string item in (IReadOnlyList<string>)array)
            {
                if (item == null)
                {
                    resultArray.AppendNull();
                    continue;
                }

                // Try to parse the value into a decimal because it is the most performant and handles the exponent syntax. But this might overflow.
                if (decimal.TryParse(item, NumberStyles.Float, CultureInfo.InvariantCulture, out decimal decimalValue))
                {
                    resultArray.Append(new SqlDecimal(decimalValue));
                }
                else
                {
                    DecimalUtility.GetBytes(item, decimalType.Precision, decimalType.Scale, decimalType.ByteWidth, buffer);
                    resultArray.Append(buffer);
                }
            }
            return resultArray.Build();
        }

        private static TimestampArray ConvertToTimestamp(StringArray array, IArrowType _)
        {
            // Match the precision of the server
            var resultArrayBuilder = new TimestampArray.Builder(TimeUnit.Microsecond);
            foreach (string item in (IReadOnlyList<string>)array)
            {
                if (item == null)
                {
                    resultArrayBuilder.AppendNull();
                    continue;
                }

                ReadOnlySpan<char> date = item.AsSpan();
                bool isKnownFormat = date.Length >= 17 && date[4] == AsciiDash && date[7] == AsciiDash && date[10] == AsciiSpace && date[13] == AsciiColon && date[16] == AsciiColon;
                if (isKnownFormat)
                {
                    DateTimeOffset value = ConvertToDateTimeOffset(date);
                    resultArrayBuilder.Append(value);
                }
                else
                {
                    DateTimeOffset value = DateTimeOffset.Parse(item, DateTimeFormatInfo.InvariantInfo, DateTimeStyles.AssumeUniversal);
                    resultArrayBuilder.Append(value);
                }
            }
            return resultArrayBuilder.Build();
        }

        private static DateTimeOffset ConvertToDateTimeOffset(ReadOnlySpan<char> date)
        {
            int year;
            int month;
            int day;
            int hour;
            int minute;
            int second;
#if NETCOREAPP
            year = int.Parse(date.Slice(0, 4));
            month = int.Parse(date.Slice(5, 2));
            day = int.Parse(date.Slice(8, 2));
            hour = int.Parse(date.Slice(11, 2));
            minute = int.Parse(date.Slice(14, 2));
            second = int.Parse(date.Slice(17, 2));
#else
            year = int.Parse(date.Slice(0, 4).ToString());
            month = int.Parse(date.Slice(5, 2).ToString());
            day = int.Parse(date.Slice(8, 2).ToString());
            hour = int.Parse(date.Slice(11, 2).ToString());
            minute = int.Parse(date.Slice(14, 2).ToString());
            second = int.Parse(date.Slice(17, 2).ToString());
#endif
            DateTimeOffset dateValue = new(year, month, day, hour, minute, second, TimeSpan.Zero);
            int length = date.Length;
            if (length >= 20 && date[19] == AsciiPeriod)
            {
                int start = -1;
                int end = 20;
                while (end < length && (uint)(date[end] - AsciiZero) <= AsciiDigitMaxIndex)
                {
                    if (start == -1) start = end;
                    end++;
                }
                int subSeconds = 0;
                int subSecondsLength = start != -1 ? end - start : 0;
                if (subSecondsLength > 0)
                {
#if NETCOREAPP
                    subSeconds = int.Parse(date.Slice(start, subSecondsLength));
#else
                    subSeconds = int.Parse(date.Slice(start, subSecondsLength).ToString());
#endif
                }
                double factorOfMilliseconds = Math.Pow(10, subSecondsLength - 3);
                long ticks = (long)(subSeconds * (TimeSpan.TicksPerMillisecond / factorOfMilliseconds));
                dateValue = dateValue.AddTicks(ticks);
            }

            return dateValue;
        }
    }
}
