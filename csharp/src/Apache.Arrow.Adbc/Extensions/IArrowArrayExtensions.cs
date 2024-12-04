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
using System.Collections;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.IO;
using System.Text.Json;
using Apache.Arrow.Types;

namespace Apache.Arrow.Adbc.Extensions
{
    public static class IArrowArrayExtensions
    {
        /// <summary>
        /// Helper extension to get a value from the <see cref="IArrowArray"/> at the specified index.
        /// </summary>
        /// <param name="arrowArray">
        /// The Arrow array.
        /// </param>
        /// <param name="index">
        /// The index in the array to get the value from.
        /// </param>
        public static object? ValueAt(this IArrowArray arrowArray, int index)
        {
            if (arrowArray == null) throw new ArgumentNullException(nameof(arrowArray));
            if (index < 0) throw new ArgumentOutOfRangeException(nameof(index));

            switch (arrowArray)
            {
                case BooleanArray booleanArray:
                    return booleanArray.GetValue(index);
                case Date32Array date32Array:
                    return date32Array.GetDateTime(index);
                case Date64Array date64Array:
                    return date64Array.GetDateTime(index);
                case Decimal128Array decimal128Array:
                    return decimal128Array.GetSqlDecimal(index);
                case Decimal256Array decimal256Array:
                    return decimal256Array.GetString(index);
                case DoubleArray doubleArray:
                    return doubleArray.GetValue(index);
                case FloatArray floatArray:
                    return floatArray.GetValue(index);
#if NET5_0_OR_GREATER
                case PrimitiveArray<Half> halfFloatArray:
                    return halfFloatArray.GetValue(index);
#endif
                case Int8Array int8Array:
                    return int8Array.GetValue(index);
                case Int16Array int16Array:
                    return int16Array.GetValue(index);
                case Int32Array int32Array:
                    return int32Array.GetValue(index);
                case Int64Array int64Array:
                    return int64Array.GetValue(index);
                case StringArray stringArray:
                    return stringArray.GetString(index);
#if NET6_0_OR_GREATER
                case Time32Array time32Array:
                    return time32Array.GetTime(index);
                case Time64Array time64Array:
                    return time64Array.GetTime(index);
#else
                case Time32Array time32Array:
                    int? time32 = time32Array.GetValue(index);
                    if (time32 == null) { return null; }
                    return ((Time32Type)time32Array.Data.DataType).Unit switch
                    {
                        TimeUnit.Second => TimeSpan.FromSeconds(time32.Value),
                        TimeUnit.Millisecond => TimeSpan.FromMilliseconds(time32.Value),
                        _ => throw new InvalidDataException("Unsupported time unit for Time32Type")
                    };
                case Time64Array time64Array:
                    long? time64 = time64Array.GetValue(index);
                    if (time64 == null) { return null; }
                    return ((Time64Type)time64Array.Data.DataType).Unit switch
                    {
                        TimeUnit.Microsecond => TimeSpan.FromTicks(time64.Value * 10),
                        TimeUnit.Nanosecond => TimeSpan.FromTicks(time64.Value / 100),
                        _ => throw new InvalidDataException("Unsupported time unit for Time64Type")
                    };
#endif
                case TimestampArray timestampArray:
                    return timestampArray.GetTimestamp(index);
                case UInt8Array uInt8Array:
                    return uInt8Array.GetValue(index);
                case UInt16Array uInt16Array:
                    return uInt16Array.GetValue(index);
                case UInt32Array uInt32Array:
                    return uInt32Array.GetValue(index);
                case UInt64Array uInt64Array:
                    return uInt64Array.GetValue(index);
                case DayTimeIntervalArray dayTimeIntervalArray:
                    return dayTimeIntervalArray.GetValue(index);
                case MonthDayNanosecondIntervalArray monthDayNanosecondIntervalArray:
                    return monthDayNanosecondIntervalArray.GetValue(index);
                case YearMonthIntervalArray yearMonthIntervalArray:
                    return yearMonthIntervalArray.GetValue(index);
                case BinaryArray binaryArray:
                    if (!binaryArray.IsNull(index))
                    {
                        return binaryArray.GetBytes(index).ToArray();
                    }
                    else
                    {
                        return null;
                    }
                case ListArray listArray:
                    return listArray.GetSlicedValues(index);
                case StructArray structArray:
                    return SerializeToJson(structArray, index);

                    // not covered:
                    // -- map array
                    // -- dictionary array
                    // -- fixed size binary
                    // -- union array
            }

            return null;
        }

        /// <summary>
        /// Converts a StructArray to a JSON string.
        /// </summary>
        private static string SerializeToJson(StructArray structArray, int index)
        {
            Dictionary<String, object?>? jsonDictionary = ParseStructArray(structArray, index);

            return JsonSerializer.Serialize(jsonDictionary);
        }

        /// <summary>
        /// Converts a StructArray to a Dictionary<String, object?>.
        /// </summary>
        private static Dictionary<String, object?>? ParseStructArray(StructArray structArray, int index)
        {
            if (structArray.IsNull(index))
                return null;

            Dictionary<String, object?> jsonDictionary = new Dictionary<String, object?>();
            StructType structType = (StructType)structArray.Data.DataType;
            for (int i = 0; i < structArray.Data.Children.Length; i++)
            {
                string name = structType.Fields[i].Name;
                object? value = ValueAt(structArray.Fields[i], index);

                if (value is StructArray structArray1)
                {
                    List<Dictionary<string, object?>?> children = new List<Dictionary<string, object?>?>();

                    for (int j = 0; j < structArray1.Length; j++)
                    {
                        children.Add(ParseStructArray(structArray1, j));
                    }

                    if (children.Count > 0)
                    {
                        jsonDictionary.Add(name, children);
                    }
                    else
                    {
                        jsonDictionary.Add(name, ParseStructArray(structArray1, index));
                    }
                }
                else if (value is IArrowArray arrowArray)
                {
                    IList? values = CreateList(arrowArray);

                    if (values != null)
                    {
                        for (int j = 0; j < arrowArray.Length; j++)
                        {
                            values.Add(ValueAt(arrowArray, j));
                        }

                        jsonDictionary.Add(name, values);
                    }
                    else
                    {
                        jsonDictionary.Add(name, new List<object>());
                    }
                }
                else
                {
                    jsonDictionary.Add(name, value);
                }
            }

            return jsonDictionary;
        }

        /// <summary>
        /// Creates a List<T> based on the type of the Arrow array.
        /// </summary>
        /// <param name="arrowArray"></param>
        /// <returns></returns>
        /// <exception cref="ArgumentNullException"></exception>
        private static IList? CreateList(IArrowArray arrowArray)
        {
            if (arrowArray == null) throw new ArgumentNullException(nameof(arrowArray));

            switch (arrowArray)
            {
                case BooleanArray booleanArray:
                    return new List<bool>();
                case Date32Array date32Array:
                case Date64Array date64Array:
                    return new List<DateTime>();
                case Decimal128Array decimal128Array:
                    return new List<SqlDecimal>();
                case Decimal256Array decimal256Array:
                    return new List<string>();
                case DoubleArray doubleArray:
                    return new List<double>();
                case FloatArray floatArray:
                    return new List<float>();
#if NET5_0_OR_GREATER
                case PrimitiveArray<Half> halfFloatArray:
                    return new List<Half>();
#endif
                case Int8Array int8Array:
                    return new List<sbyte>();
                case Int16Array int16Array:
                    return new List<short>();
                case Int32Array int32Array:
                    return new List<int>();
                case Int64Array int64Array:
                    return new List<long>();
                case StringArray stringArray:
                    return new List<string>();
#if NET6_0_OR_GREATER
                case Time32Array time32Array:
                case Time64Array time64Array:
                    return new List<TimeOnly>();
#else
                case Time32Array time32Array:
                case Time64Array time64Array:
                    return new List<TimeSpan>();
#endif
                case TimestampArray timestampArray:
                    return new List<DateTimeOffset>();
                case UInt8Array uInt8Array:
                    return new List<byte>();
                case UInt16Array uInt16Array:
                    return new List<ushort>();
                case UInt32Array uInt32Array:
                    return new List<uint>();
                case UInt64Array uInt64Array:
                    return new List<ulong>();

                case BinaryArray binaryArray:
                    return new List<byte>();

                    // not covered:
                    // -- struct array
                    // -- dictionary array
                    // -- fixed size binary
                    // -- list array
                    // -- union array
            }

            return null;
        }
    }
}
