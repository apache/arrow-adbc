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
using Thrift.Transport;

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
        private const int KnownFormatDateLength = 10;
        private const int KnownFormatDateTimeLength = 19;
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
        private readonly DataTypeConversion _dataTypeConversion;
        private static readonly IReadOnlyDictionary<ArrowTypeId, Func<StringArray, IArrowType, IArrowArray>> s_arrowStringConverters =
            new Dictionary<ArrowTypeId, Func<StringArray, IArrowType, IArrowArray>>()
            {
                { ArrowTypeId.Date32, ConvertToDate32 },
                { ArrowTypeId.Decimal128, ConvertToDecimal128 },
                { ArrowTypeId.Timestamp, ConvertToTimestamp },
            };
        private static readonly IReadOnlyDictionary<ArrowTypeId, Func<DoubleArray, IArrowType, IArrowArray>> s_arrowDoubleConverters =
            new Dictionary<ArrowTypeId, Func<DoubleArray, IArrowType, IArrowArray>>()
            {
                { ArrowTypeId.Float, ConvertToFloat },
            };

        public HiveServer2Reader(
            HiveServer2Statement statement,
            Schema schema,
            DataTypeConversion dataTypeConversion)
        {
            _statement = statement;
            Schema = schema;
            _dataTypeConversion = dataTypeConversion;
        }

        public Schema Schema { get; }

        public async ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
        {
            // All records have been exhausted
            if (_statement == null)
            {
                return null;
            }

            try
            {
                // Await the fetch response
                TFetchResultsResp response = await FetchNext(_statement, cancellationToken);

                int columnCount = GetColumnCount(response);
                int rowCount = GetRowCount(response, columnCount);
                if ((_statement.BatchSize > 0 && rowCount < _statement.BatchSize) || rowCount == 0)
                {
                    // This is the last batch
                    _statement = null;
                }

                // Build the current batch, if any data exists
                return rowCount > 0 ? CreateBatch(response, columnCount, rowCount) : null;
            }
            catch (Exception ex)
                when (ApacheUtility.ContainsException(ex, out OperationCanceledException? _) ||
                     (ApacheUtility.ContainsException(ex, out TTransportException? _) && cancellationToken.IsCancellationRequested))
            {
                throw new TimeoutException("The query execution timed out. Consider increasing the query timeout value.", ex);
            }
            catch (Exception ex) when (ex is not HiveServer2Exception)
            {
                throw new HiveServer2Exception($"An unexpected error occurred while fetching results. '{ex.Message}'", ex);
            }
        }

        private RecordBatch CreateBatch(TFetchResultsResp response, int columnCount, int rowCount)
        {
            IList<IArrowArray> columnData = [];
            bool shouldConvertScalar = _dataTypeConversion.HasFlag(DataTypeConversion.Scalar);
            for (int i = 0; i < columnCount; i++)
            {
                IArrowType? expectedType = shouldConvertScalar ? Schema.FieldsList[i].DataType : null;
                IArrowArray columnArray = GetArray(response.Results.Columns[i], expectedType);
                columnData.Add(columnArray);
            }

            return new RecordBatch(Schema, columnData, rowCount);
        }

        private static int GetColumnCount(TFetchResultsResp response) =>
            response.Results.Columns.Count;

        private static int GetRowCount(TFetchResultsResp response, int columnCount) =>
            columnCount > 0 ? GetArray(response.Results.Columns[0]).Length : 0;

        private static async Task<TFetchResultsResp> FetchNext(HiveServer2Statement statement, CancellationToken cancellationToken = default)
        {
            var request = new TFetchResultsReq(statement.OperationHandle, TFetchOrientation.FETCH_NEXT, statement.BatchSize);
            return await statement.Connection.Client.FetchResults(request, cancellationToken);
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
            else if (expectedArrowType != null && arrowArray is DoubleArray doubleArray && s_arrowDoubleConverters.ContainsKey(expectedArrowType.TypeId))
            {
                // Perform a conversion from double to another (float) type.
                Func<DoubleArray, IArrowType, IArrowArray> converter = s_arrowDoubleConverters[expectedArrowType.TypeId];
                return converter(doubleArray, expectedArrowType);
            }
            return arrowArray;
        }

        internal static Date32Array ConvertToDate32(StringArray array, IArrowType _)
        {
            const DateTimeStyles DateTimeStyles = DateTimeStyles.AllowWhiteSpaces;
            int length = array.Length;
            var resultArray = new Date32Array
                .Builder()
                .Reserve(length);
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
                    || DateTime.TryParse(array.GetString(i), CultureInfo.InvariantCulture, DateTimeStyles, out dateTime))
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

        internal static FloatArray ConvertToFloat(DoubleArray array, IArrowType _)
        {
            int length = array.Length;
            var resultArray = new FloatArray
                .Builder()
                .Reserve(length);
            for (int i = 0; i < length; i++)
            {
                resultArray.Append((float?)array.GetValue(i));
            }

            return resultArray.Build();
        }

        internal static bool TryParse(ReadOnlySpan<byte> date, out DateTime dateTime)
        {
            if (date.Length == KnownFormatDateLength
                && date[YearMonthSepIndex] == AsciiDash && date[MonthDaySepIndex] == AsciiDash
                && Utf8Parser.TryParse(date.Slice(YearIndex, 4), out int year, out int bytesConsumed) && bytesConsumed == 4
                && Utf8Parser.TryParse(date.Slice(MonthIndex, 2), out int month, out bytesConsumed) && bytesConsumed == 2
                && Utf8Parser.TryParse(date.Slice(DayIndex, 2), out int day, out bytesConsumed) && bytesConsumed == 2)
            {
                try
                {
                    dateTime = new(year, month, day);
                    return true;
                }
                catch (ArgumentOutOfRangeException)
                {
                    dateTime = default;
                    return false;
                }
            }

            dateTime = default;
            return false;
        }

        private static Decimal128Array ConvertToDecimal128(StringArray array, IArrowType schemaType)
        {
            int length = array.Length;
            // Using the schema type to get the precision and scale.
            Decimal128Type decimalType = (Decimal128Type)schemaType;
            var resultArray = new Decimal128Array
                .Builder(decimalType)
                .Reserve(length);
            Span<byte> buffer = stackalloc byte[decimalType.ByteWidth];

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

        internal static TimestampArray ConvertToTimestamp(StringArray array, IArrowType _)
        {
            const DateTimeStyles DateTimeStyles = DateTimeStyles.AssumeUniversal | DateTimeStyles.AllowWhiteSpaces;
            int length = array.Length;
            // Match the precision of the server
            var resultArrayBuilder = new TimestampArray
                .Builder(TimeUnit.Microsecond)
                .Reserve(length);
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
                    || DateTimeOffset.TryParse(array.GetString(i), CultureInfo.InvariantCulture, DateTimeStyles, out dateValue))
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

        internal static bool TryParse(ReadOnlySpan<byte> date, out DateTimeOffset dateValue)
        {
            bool isKnownFormat = date.Length >= KnownFormatDateTimeLength
                && date[YearMonthSepIndex] == AsciiDash
                && date[MonthDaySepIndex] == AsciiDash
                && date[DayHourSepIndex] == AsciiSpace
                && date[HourMinuteSepIndex] == AsciiColon
                && date[MinuteSecondSepIndex] == AsciiColon;

            if (!isKnownFormat
                || !Utf8Parser.TryParse(date.Slice(YearIndex, 4), out int year, out int bytesConsumed, standardFormat: 'D') || bytesConsumed != 4
                || !Utf8Parser.TryParse(date.Slice(MonthIndex, 2), out int month, out bytesConsumed, standardFormat: 'D') || bytesConsumed != 2
                || !Utf8Parser.TryParse(date.Slice(DayIndex, 2), out int day, out bytesConsumed, standardFormat: 'D') || bytesConsumed != 2
                || !Utf8Parser.TryParse(date.Slice(HourIndex, 2), out int hour, out bytesConsumed, standardFormat: 'D') || bytesConsumed != 2
                || !Utf8Parser.TryParse(date.Slice(MinuteIndex, 2), out int minute, out bytesConsumed, standardFormat: 'D') || bytesConsumed != 2
                || !Utf8Parser.TryParse(date.Slice(SecondIndex, 2), out int second, out bytesConsumed, standardFormat: 'D') || bytesConsumed != 2)
            {
                dateValue = default;
                return false;
            }

            try
            {
                dateValue = new(year, month, day, hour, minute, second, TimeSpan.Zero);
            }
            catch (ArgumentOutOfRangeException)
            {
                dateValue = default;
                return false;
            }

            // Retrieve subseconds, if available
            int length = date.Length;
            if (length > SecondSubsecondSepIndex)
            {
                if (date[SecondSubsecondSepIndex] == AsciiPeriod)
                {
                    int start = -1;
                    int end = SubsecondIndex;
                    while (end < length && (uint)(date[end] - AsciiZero) <= AsciiDigitMaxIndex)
                    {
                        if (start == -1) start = end;
                        end++;
                    }
                    if (end < length)
                    {
                        // Indicates unrecognized trailing character(s)
                        dateValue = default;
                        return false;
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
                else
                {
                    dateValue = default;
                    return false;
                }
            }

            return true;
        }
    }
}
