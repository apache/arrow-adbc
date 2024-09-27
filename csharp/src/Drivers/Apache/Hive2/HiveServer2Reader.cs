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
using System.Buffers.Text;
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
        private const byte AsciiZero = (byte)'0';
        private const int AsciiDigitMaxIndex = '9' - AsciiZero;
        private const byte AsciiDash = (byte)'-';
        private const byte AsciiSpace = (byte)' ';
        private const byte AsciiColon = (byte)':';
        private const byte AsciiPeriod = (byte)'.';
        private const char StandardFormatRoundTrippable = 'O';
        private const char StandardFormatExponent = 'E';
        private const int YearMonthSepIndex = 4;
        private const int MonthDaySepIndex = 7;
        private const int KnownFormatLength = 19;
        private const int DayHourSepIndex = 10;
        private const int HourMinuteSepIndex = 13;
        private const int MinuteSecondSepIndex = 16;
        private const int YearIndex = 0;
        private const int MonthIndex = 5;
        private const int DayIndex = 8;
        private const int HourIndex = 11;
        private const int MinuteIndex = 14;
        private const int SecondIndex = 17;
        private const int SecondSubsecondSepIndex = 19;
        private const int SubsecondIndex = 20;
        private const int MillisecondDecimalPlaces = 3;

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
            int length = array.Length;
            for (int i = 0; i < length; i++)
            {
                // Work with UTF8 string.
                ReadOnlySpan<byte> date = array.GetBytes(i, out bool isNull);
                if (isNull)
                {
                    resultArray.AppendNull();

                }
                else if (TryParse(date, out DateTime dateTime)
                    || Utf8Parser.TryParse(date, out dateTime, out int _, standardFormat: StandardFormatRoundTrippable)
                    || DateTime.TryParse(array.GetString(i), CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out dateTime))
                {
                    resultArray.Append(dateTime);
                }
                else
                {
                    throw new FormatException($"unable to convert value '{array.GetString(i)}' to DateTime");
                }
            }

            return resultArray.Build();
        }

        private static bool TryParse(ReadOnlySpan<byte> date, out DateTime dateTime)
        {
            if (date.Length >= 10 && date[YearMonthSepIndex] == AsciiDash && date[MonthDaySepIndex] == AsciiDash
                && Utf8Parser.TryParse(date.Slice(YearIndex), out int year, out _)
                && Utf8Parser.TryParse(date.Slice(MonthIndex), out int month, out _)
                && Utf8Parser.TryParse(date.Slice(DayIndex), out int day, out _))
            {
                dateTime = new(year, month, day);
                return true;
            }

            dateTime = default;
            return false;
        }

        private static Decimal128Array ConvertToDecimal128(StringArray array, IArrowType schemaType)
        {
            // Using the schema type to get the precision and scale.
            Decimal128Type decimalType = (Decimal128Type)schemaType;
            var resultArray = new Decimal128Array.Builder(decimalType);
            Span<byte> buffer = stackalloc byte[decimalType.ByteWidth];

            int length = array.Length;
            for (int i = 0; i < length; i++)
            {
                // Work with UTF8 string.
                ReadOnlySpan<byte> item = array.GetBytes(i, out bool isNull);
                if (isNull)
                {
                    resultArray.AppendNull();
                }
                // Try to parse the value into a decimal because it is the most performant and handles the exponent syntax. But this might overflow.
                else if (Utf8Parser.TryParse(item, out decimal decimalValue, out int _, standardFormat: StandardFormatExponent))
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
            int length = array.Length;
            for (int i = 0; i < length; i++)
            {
                // Work with UTF8 string.
                ReadOnlySpan<byte> date = array.GetBytes(i, out bool isNull);
                if (isNull)
                {
                    resultArrayBuilder.AppendNull();
                }
                else if (TryParse(date, out DateTimeOffset dateValue)
                    || Utf8Parser.TryParse(date, out dateValue, out int _, standardFormat: StandardFormatRoundTrippable)
                    || DateTimeOffset.TryParse(array.GetString(i), CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out dateValue))
                {
                    resultArrayBuilder.Append(dateValue);
                }
                else
                {
                    throw new FormatException($"unable to convert value '{array.GetString(i)}' to DateTimeOffset");
                }
            }

            return resultArrayBuilder.Build();
        }

        private static bool TryParse(ReadOnlySpan<byte> date, out DateTimeOffset dateValue)
        {
            bool isKnownFormat = date.Length >= KnownFormatLength
                && date[YearMonthSepIndex] == AsciiDash
                && date[MonthDaySepIndex] == AsciiDash
                && date[DayHourSepIndex] == AsciiSpace
                && date[HourMinuteSepIndex] == AsciiColon
                && date[MinuteSecondSepIndex] == AsciiColon;

            if (!isKnownFormat
                || !Utf8Parser.TryParse(date.Slice(YearIndex), out int year, out _)
                || !Utf8Parser.TryParse(date.Slice(MonthIndex), out int month, out _)
                || !Utf8Parser.TryParse(date.Slice(DayIndex), out int day, out _)
                || !Utf8Parser.TryParse(date.Slice(HourIndex), out int hour, out _)
                || !Utf8Parser.TryParse(date.Slice(MinuteIndex), out int minute, out _)
                || !Utf8Parser.TryParse(date.Slice(SecondIndex), out int second, out _))
            {
                dateValue = default;
                return false;
            }

            dateValue = new(year, month, day, hour, minute, second, TimeSpan.Zero);

            // Retrieve subseconds, if available
            int length = date.Length;
            if (length > SubsecondIndex && date[SecondSubsecondSepIndex] == AsciiPeriod)
            {
                int start = -1;
                int end = SubsecondIndex;
                while (end < length && (uint)(date[end] - AsciiZero) <= AsciiDigitMaxIndex)
                {
                    if (start == -1) start = end;
                    end++;
                }

                int subSecondsLength = start != -1 ? end - start : 0;
                if (subSecondsLength > 0)
                {
                    if (!Utf8Parser.TryParse(date.Slice(start, subSecondsLength), out int subSeconds, out _))
                    {
                        dateValue = default;
                        return false;
                    }

                    double factorOfMilliseconds = Math.Pow(10, subSecondsLength - MillisecondDecimalPlaces);
                    long ticks = (long)(subSeconds * (TimeSpan.TicksPerMillisecond / factorOfMilliseconds));
                    dateValue = dateValue.AddTicks(ticks);
                }
            }

            return true;
        }
    }
}
