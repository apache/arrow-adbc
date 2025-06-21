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
    public enum StructResultType
    {
        JsonString,
        Object
    }

    public static class IArrowArrayExtensions
    {
        /// <summary>
        /// Overloaded. Helper extension to get a value from the <see cref="IArrowArray"/> at the specified index.
        /// </summary>
        /// <param name="arrowArray">
        /// The Arrow array.
        /// </param>
        /// <param name="index">
        /// The index in the array to get the value from.
        /// </param>
        public static object? ValueAt(this IArrowArray arrowArray, int index)
        {
            return ValueAt(arrowArray, index, StructResultType.JsonString);
        }

        /// <summary>
        /// Overloaded. Helper extension to get a value from the <see cref="IArrowArray"/> at the specified index.
        /// </summary>
        /// <param name="arrowArray">
        /// The Arrow array.
        /// </param>
        /// <param name="index">
        /// The index in the array to get the value from.
        /// </param>
        /// <param name="resultType">
        /// T
        /// </param>
        public static object? ValueAt(this IArrowArray arrowArray, int index, StructResultType resultType = StructResultType.JsonString)
        {
            if (arrowArray == null) throw new ArgumentNullException(nameof(arrowArray));
            if (index < 0) throw new ArgumentOutOfRangeException(nameof(index));

            if (arrowArray.IsNull(index))
                return null;

            switch (arrowArray.Data.DataType.TypeId)
            {
                case ArrowTypeId.Null:
                    return null;
                case ArrowTypeId.Boolean:
                    return ((BooleanArray)arrowArray).GetValue(index);
                case ArrowTypeId.Date32:
                    return ((Date32Array)arrowArray).GetDateTime(index);
                case ArrowTypeId.Date64:
                    return ((Date64Array)arrowArray).GetDateTime(index);
                case ArrowTypeId.Decimal32:
                    return ((Decimal32Array)arrowArray).GetDecimal(index);
                case ArrowTypeId.Decimal64:
                    return ((Decimal64Array)arrowArray).GetDecimal(index);
                case ArrowTypeId.Decimal128:
                    return ((Decimal128Array)arrowArray).GetSqlDecimal(index);
                case ArrowTypeId.Decimal256:
                    return ((Decimal256Array)arrowArray).GetString(index);
                case ArrowTypeId.Double:
                    return ((DoubleArray)arrowArray).GetValue(index);
                case ArrowTypeId.Float:
                    return ((FloatArray)arrowArray).GetValue(index);
#if NET5_0_OR_GREATER
                case ArrowTypeId.HalfFloat:
                    return ((HalfFloatArray)arrowArray).GetValue(index);
#endif
                case ArrowTypeId.Int8:
                    return ((Int8Array)arrowArray).GetValue(index);
                case ArrowTypeId.Int16:
                    return ((Int16Array)arrowArray).GetValue(index);
                case ArrowTypeId.Int32:
                    return ((Int32Array)arrowArray).GetValue(index);
                case ArrowTypeId.Int64:
                    return ((Int64Array)arrowArray).GetValue(index);
                case ArrowTypeId.String:
                    return ((StringArray)arrowArray).GetString(index);
                case ArrowTypeId.LargeString:
                    return ((LargeStringArray)arrowArray).GetString(index);
#if NET6_0_OR_GREATER
                case ArrowTypeId.Time32:
                    return ((Time32Array)arrowArray).GetTime(index);
                case ArrowTypeId.Time64:
                    return ((Time64Array)arrowArray).GetTime(index);
#else
                case ArrowTypeId.Time32:
                    Time32Array time32Array = (Time32Array)arrowArray;
                    int? time32 = time32Array.GetValue(index);
                    if (time32 == null) { return null; }
                    return ((Time32Type)time32Array.Data.DataType).Unit switch
                    {
                        TimeUnit.Second => TimeSpan.FromSeconds(time32.Value),
                        TimeUnit.Millisecond => TimeSpan.FromMilliseconds(time32.Value),
                        _ => throw new InvalidDataException("Unsupported time unit for Time32Type")
                    };
                case ArrowTypeId.Time64:
                    Time64Array time64Array = (Time64Array)arrowArray;
                    long? time64 = time64Array.GetValue(index);
                    if (time64 == null) { return null; }
                    return ((Time64Type)time64Array.Data.DataType).Unit switch
                    {
                        TimeUnit.Microsecond => TimeSpan.FromTicks(time64.Value * 10),
                        TimeUnit.Nanosecond => TimeSpan.FromTicks(time64.Value / 100),
                        _ => throw new InvalidDataException("Unsupported time unit for Time64Type")
                    };
#endif
                case ArrowTypeId.Timestamp:
                    return ((TimestampArray)arrowArray).GetTimestamp(index);
                case ArrowTypeId.UInt8:
                    return ((UInt8Array)arrowArray).GetValue(index);
                case ArrowTypeId.UInt16:
                    return ((UInt16Array)arrowArray).GetValue(index);
                case ArrowTypeId.UInt32:
                    return ((UInt32Array)arrowArray).GetValue(index);
                case ArrowTypeId.UInt64:
                    return ((UInt64Array)arrowArray).GetValue(index);
                case ArrowTypeId.Interval:
                    switch (((IntervalType)arrowArray.Data.DataType).Unit)
                    {
                        case IntervalUnit.DayTime:
                            return ((DayTimeIntervalArray)arrowArray).GetValue(index);
                        case IntervalUnit.MonthDayNanosecond:
                            return ((MonthDayNanosecondIntervalArray)arrowArray).GetValue(index);
                        case IntervalUnit.YearMonth:
                            return ((YearMonthIntervalArray)arrowArray).GetValue(index);
                        default:
                            throw new NotSupportedException($"Unsupported interval unit: {((IntervalType)arrowArray.Data.DataType).Unit}");
                    }
                case ArrowTypeId.Binary:
                    return ((BinaryArray)arrowArray).GetBytes(index).ToArray();
                case ArrowTypeId.List:
                    return ((ListArray)arrowArray).GetSlicedValues(index);
                case ArrowTypeId.Struct:
                    StructArray structArray = (StructArray)arrowArray;
                    return resultType == StructResultType.JsonString ? SerializeToJson(structArray, index) : ParseStructArray(structArray, index);

                    // not covered:
                    // -- map array
                    // -- dictionary array
                    // -- fixed size binary
                    // -- union array
            }

            return null;
        }

        /// <summary>
        /// Overloaded. Helper extension to get a value converter for the <see href="IArrowType"/>.
        /// </summary>
        /// <param name="arrayType">
        /// The return type of an item in a StructArray.
        /// </param>
        public static Func<IArrowArray, int, object?> GetValueConverter(this IArrowType arrayType)
        {
            return GetValueConverter(arrayType, StructResultType.JsonString);
        }

        /// <summary>
        /// Overloaded. Helper extension to get a value from the <see cref="IArrowArray"/> at the specified index.
        /// </summary>
        /// <param name="arrayType">
        /// The Arrow array type.
        /// </param>
        /// <param name="sourceType">
        /// The incoming <see cref="SourceStringType"/>.
        /// </param>
        /// <param name="resultType">
        /// The return type of an item in a StructArray.
        /// </param>
        public static Func<IArrowArray, int, object?> GetValueConverter(this IArrowType arrayType, StructResultType resultType)
        {
            if (arrayType == null) throw new ArgumentNullException(nameof(arrayType));

            switch (arrayType.TypeId)
            {
                case ArrowTypeId.Null:
                    return (array, index) => null;
                case ArrowTypeId.Boolean:
                    return (array, index) => ((BooleanArray)array).GetValue(index);
                case ArrowTypeId.Date32:
                    return (array, index) => ((Date32Array)array).GetDateTime(index);
                case ArrowTypeId.Date64:
                    return (array, index) => ((Date64Array)array).GetDateTime(index);
                case ArrowTypeId.Decimal32:
                    return (array, index) => ((Decimal32Array)array).GetDecimal(index);
                case ArrowTypeId.Decimal64:
                    return (array, index) => ((Decimal64Array)array).GetDecimal(index);
                case ArrowTypeId.Decimal128:
                    return (array, index) => ((Decimal128Array)array).GetSqlDecimal(index);
                case ArrowTypeId.Decimal256:
                    return (array, index) => ((Decimal256Array)array).GetString(index);
                case ArrowTypeId.Double:
                    return (array, index) => ((DoubleArray)array).GetValue(index);
                case ArrowTypeId.Float:
                    return (array, index) => ((FloatArray)array).GetValue(index);
#if NET5_0_OR_GREATER
                case ArrowTypeId.HalfFloat:
                    return (array, index) => ((HalfFloatArray)array).GetValue(index);
#endif
                case ArrowTypeId.Int8:
                    return (array, index) => ((Int8Array)array).GetValue(index);
                case ArrowTypeId.Int16:
                    return (array, index) => ((Int16Array)array).GetValue(index);
                case ArrowTypeId.Int32:
                    return (array, index) => ((Int32Array)array).GetValue(index);
                case ArrowTypeId.Int64:
                    return (array, index) => ((Int64Array)array).GetValue(index);
                case ArrowTypeId.String:
                    return (array, index) => array.Data.DataType.TypeId == ArrowTypeId.Decimal256 ?
                        ((Decimal256Array)array).GetString(index) :
                        ((StringArray)array).GetString(index);
                case ArrowTypeId.LargeString:
                    return (array, index) =>((LargeStringArray)array).GetString(index);
#if NET6_0_OR_GREATER
                case ArrowTypeId.Time32:
                    return (array, index) => ((Time32Array)array).GetTime(index);
                case ArrowTypeId.Time64:
                    return (array, index) => ((Time64Array)array).GetTime(index);
#else
                case ArrowTypeId.Time32:
                    Time32Type time32Type = (Time32Type)arrayType;
                    switch (time32Type.Unit)
                    {
                        case TimeUnit.Second:
                            return (array, index) => array.IsNull(index) ? null : TimeSpan.FromSeconds(((Time32Array)array).GetValue(index)!.Value);
                        case TimeUnit.Millisecond:
                            return (array, index) => array.IsNull(index) ? null : TimeSpan.FromMilliseconds(((Time32Array)array).GetValue(index)!.Value);
                        default:
                            throw new InvalidDataException("Unsupported time unit for Time32Type");
                    }
                case ArrowTypeId.Time64:
                    Time64Type time64Type = (Time64Type)arrayType;
                    switch (time64Type.Unit)
                    {
                        case TimeUnit.Microsecond:
                            return (array, index) => array.IsNull(index) ? null : TimeSpan.FromTicks(((Time64Array)array).GetValue(index)!.Value * 10);
                        case TimeUnit.Nanosecond:
                            return (array, index) => array.IsNull(index) ? null : TimeSpan.FromTicks(((Time64Array)array).GetValue(index)!.Value / 100);
                        default:
                            throw new InvalidDataException("Unsupported time unit for Time64Type");
                    }
#endif
                case ArrowTypeId.Timestamp:
                    return (array, index) => ((TimestampArray)array).GetTimestamp(index);
                case ArrowTypeId.UInt8:
                    return (array, index) => ((UInt8Array)array).GetValue(index);
                case ArrowTypeId.UInt16:
                    return (array, index) => ((UInt16Array)array).GetValue(index);
                case ArrowTypeId.UInt32:
                    return (array, index) => ((UInt32Array)array).GetValue(index);
                case ArrowTypeId.UInt64:
                    return (array, index) => ((UInt64Array)array).GetValue(index);
                case ArrowTypeId.Interval:
                    IntervalType intervalType = (IntervalType)arrayType;
                    switch (intervalType.Unit)
                    {
                        case IntervalUnit.DayTime:
                            return (array, index) => ((DayTimeIntervalArray)array).GetValue(index);
                        case IntervalUnit.MonthDayNanosecond:
                            return (array, index) => ((MonthDayNanosecondIntervalArray)array).GetValue(index);
                        case IntervalUnit.YearMonth:
                            return (array, index) => ((YearMonthIntervalArray)array).GetValue(index);
                        default:
                            throw new NotSupportedException($"Unsupported interval unit: {intervalType.Unit}");
                    }
                case ArrowTypeId.Binary:
                    return (array, index) => array.IsNull(index) ? null : ((BinaryArray)array).GetBytes(index).ToArray();
                case ArrowTypeId.List:
                    return (array, index) => ((ListArray)array).GetSlicedValues(index);
                case ArrowTypeId.Struct:
                    return resultType == StructResultType.JsonString ?
                        (array, index) => SerializeToJson((StructArray)array, index) :
                        (array, index) => ParseStructArray((StructArray)array, index);

                    // not covered:
                    // -- map array
                    // -- dictionary array
                    // -- fixed size binary
                    // -- union array
            }

            throw new NotSupportedException($"Unsupported ArrowTypeId: {arrayType.TypeId}");
        }

        /// <summary>
        /// Converts a StructArray to a JSON string.
        /// </summary>
        private static string SerializeToJson(StructArray structArray, int index)
        {
            Dictionary<string, object?>? obj = ParseStructArray(structArray, index);

            return JsonSerializer.Serialize(obj);
        }

        /// <summary>
        /// Converts an item in the StructArray at the index position to a Dictionary<string, object?>.
        /// </summary>
        private static Dictionary<string, object?>? ParseStructArray(StructArray structArray, int index)
        {
            if (structArray.IsNull(index))
                return null;

            Dictionary<string, object?> jsonDictionary = new Dictionary<string, object?>();

            StructType structType = (StructType)structArray.Data.DataType;
            for (int i = 0; i < structArray.Data.Children.Length; i++)
            {
                string name = structType.Fields[i].Name;

                // keep the results as StructArray internally
                object? value = ValueAt(structArray.Fields[i], index, StructResultType.Object);

                if (value is StructArray structArray1)
                {
                    if (structArray1.Length == 0)
                    {
                        jsonDictionary.Add(name, null);
                    }
                    else
                    {
                        List<Dictionary<string, object?>?> children = new List<Dictionary<string, object?>?>();

                        for (int j = 0; j < structArray1.Length; j++)
                        {
                            children.Add(ParseStructArray(structArray1, j));
                        }

                        jsonDictionary.Add(name, children);
                    }
                }
                else if (value is IArrowArray arrowArray)
                {
                    IList? values = CreateList(arrowArray);

                    if (values != null)
                    {
                        for (int j = 0; j < arrowArray.Length; j++)
                        {
                            values.Add(ValueAt(arrowArray, j, StructResultType.Object));
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
