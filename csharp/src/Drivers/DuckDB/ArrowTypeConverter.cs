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
using Apache.Arrow;
using Apache.Arrow.Types;

namespace Apache.Arrow.Adbc.Drivers.DuckDB
{
    /// <summary>
    /// Converts DuckDB data types to Arrow arrays.
    /// </summary>
    public class ArrowTypeConverter
    {
        public IArrowArray ConvertToArrowArray(List<object?> values, IArrowType arrowType)
        {
            // Debug: Log the conversion
            if (values.Count > 0 && values[0] != null)
            {
                var firstValue = values[0];
                if (firstValue.GetType() == typeof(decimal) && arrowType is DoubleType)
                {
                    // Special case: DuckDB returns decimal for numeric literals but we want double
                    return ConvertToDoubleArray(values);
                }
            }
            
            return arrowType switch
            {
                BooleanType => ConvertToBooleanArray(values),
                Int8Type => ConvertToInt8Array(values),
                Int16Type => ConvertToInt16Array(values),
                Int32Type => ConvertToInt32Array(values),
                Int64Type => ConvertToInt64Array(values),
                UInt8Type => ConvertToUInt8Array(values),
                UInt16Type => ConvertToUInt16Array(values),
                UInt32Type => ConvertToUInt32Array(values),
                UInt64Type => ConvertToUInt64Array(values),
                FloatType => ConvertToFloatArray(values),
                DoubleType => ConvertToDoubleArray(values),
                StringType => ConvertToStringArray(values),
                BinaryType => ConvertToBinaryArray(values),
                Date32Type => ConvertToDate32Array(values),
                Time64Type time64 => ConvertToTime64Array(values, time64),
                TimestampType timestamp => ConvertToTimestampArray(values, timestamp),
                Decimal128Type decimal128 => ConvertToDecimal128Array(values, decimal128),
                FixedSizeBinaryType fixedBinary => ConvertToFixedSizeBinaryArray(values, fixedBinary),
                _ => throw new NotSupportedException($"Arrow type {arrowType} is not yet supported")
            };
        }

        private BooleanArray ConvertToBooleanArray(List<object?> values)
        {
            var builder = new BooleanArray.Builder();
            foreach (var value in values)
            {
                if (value == null)
                    builder.AppendNull();
                else
                    builder.Append(Convert.ToBoolean(value));
            }
            return builder.Build();
        }

        private Int8Array ConvertToInt8Array(List<object?> values)
        {
            var builder = new Int8Array.Builder();
            foreach (var value in values)
            {
                if (value == null)
                    builder.AppendNull();
                else
                    builder.Append(Convert.ToSByte(value));
            }
            return builder.Build();
        }

        private Int16Array ConvertToInt16Array(List<object?> values)
        {
            var builder = new Int16Array.Builder();
            foreach (var value in values)
            {
                if (value == null)
                    builder.AppendNull();
                else
                    builder.Append(Convert.ToInt16(value));
            }
            return builder.Build();
        }

        private Int32Array ConvertToInt32Array(List<object?> values)
        {
            var builder = new Int32Array.Builder();
            foreach (var value in values)
            {
                if (value == null)
                    builder.AppendNull();
                else
                    builder.Append(Convert.ToInt32(value));
            }
            return builder.Build();
        }

        private Int64Array ConvertToInt64Array(List<object?> values)
        {
            var builder = new Int64Array.Builder();
            foreach (var value in values)
            {
                if (value == null)
                    builder.AppendNull();
                else
                    builder.Append(Convert.ToInt64(value));
            }
            return builder.Build();
        }

        private UInt8Array ConvertToUInt8Array(List<object?> values)
        {
            var builder = new UInt8Array.Builder();
            foreach (var value in values)
            {
                if (value == null)
                    builder.AppendNull();
                else
                    builder.Append(Convert.ToByte(value));
            }
            return builder.Build();
        }

        private UInt16Array ConvertToUInt16Array(List<object?> values)
        {
            var builder = new UInt16Array.Builder();
            foreach (var value in values)
            {
                if (value == null)
                    builder.AppendNull();
                else
                    builder.Append(Convert.ToUInt16(value));
            }
            return builder.Build();
        }

        private UInt32Array ConvertToUInt32Array(List<object?> values)
        {
            var builder = new UInt32Array.Builder();
            foreach (var value in values)
            {
                if (value == null)
                    builder.AppendNull();
                else
                    builder.Append(Convert.ToUInt32(value));
            }
            return builder.Build();
        }

        private UInt64Array ConvertToUInt64Array(List<object?> values)
        {
            var builder = new UInt64Array.Builder();
            foreach (var value in values)
            {
                if (value == null)
                    builder.AppendNull();
                else
                    builder.Append(Convert.ToUInt64(value));
            }
            return builder.Build();
        }

        private FloatArray ConvertToFloatArray(List<object?> values)
        {
            var builder = new FloatArray.Builder();
            foreach (var value in values)
            {
                if (value == null)
                    builder.AppendNull();
                else
                    builder.Append(Convert.ToSingle(value));
            }
            return builder.Build();
        }

        private DoubleArray ConvertToDoubleArray(List<object?> values)
        {
            var builder = new DoubleArray.Builder();
            foreach (var value in values)
            {
                if (value == null)
                    builder.AppendNull();
                else
                    builder.Append(Convert.ToDouble(value));
            }
            return builder.Build();
        }

        private StringArray ConvertToStringArray(List<object?> values)
        {
            var builder = new StringArray.Builder();
            foreach (var value in values)
            {
                if (value == null)
                    builder.AppendNull();
                else
                    builder.Append(value.ToString());
            }
            return builder.Build();
        }

        private BinaryArray ConvertToBinaryArray(List<object?> values)
        {
            var builder = new BinaryArray.Builder();
            foreach (var value in values)
            {
                if (value == null)
                {
                    builder.AppendNull();
                }
                else if (value is byte[] bytes)
                {
                    builder.Append(bytes.AsSpan());
                }
                else if (value is System.IO.Stream stream)
                {
                    using (var ms = new System.IO.MemoryStream())
                    {
                        stream.CopyTo(ms);
                        builder.Append(ms.ToArray().AsSpan());
                    }
                }
                else
                {
                    throw new InvalidCastException($"Cannot convert {value.GetType()} to binary");
                }
            }
            return builder.Build();
        }

        private Date32Array ConvertToDate32Array(List<object?> values)
        {
            var builder = new Date32Array.Builder();
            var epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            
            foreach (var value in values)
            {
                if (value == null)
                {
                    builder.AppendNull();
                }
                else
                {
                    DateTime date;
                    if (value is DateTime dt)
                        date = dt;
#if NET6_0_OR_GREATER
                    else if (value is DateOnly dateOnly)
                        date = dateOnly.ToDateTime(TimeOnly.MinValue);
#endif
                    else
                        date = Convert.ToDateTime(value);
                    
                    builder.Append(date);
                }
            }
            return builder.Build();
        }

        private Time64Array ConvertToTime64Array(List<object?> values, Time64Type timeType)
        {
            var builder = new Time64Array.Builder(timeType);
            
            foreach (var value in values)
            {
                if (value == null)
                {
                    builder.AppendNull();
                }
                else
                {
                    TimeSpan time;
                    if (value is TimeSpan ts)
                        time = ts;
#if NET6_0_OR_GREATER
                    else if (value is TimeOnly timeOnly)
                        time = timeOnly.ToTimeSpan();
#endif
                    else if (value is DateTime dt)
                        time = dt.TimeOfDay;
                    else
                        time = TimeSpan.Parse(value.ToString()!);
                    
                    long ticks = timeType.Unit == TimeUnit.Microsecond 
                        ? time.Ticks / 10 // Convert to microseconds
                        : time.Ticks / 10000; // Convert to nanoseconds
                    
                    builder.Append(ticks);
                }
            }
            return builder.Build();
        }

        private TimestampArray ConvertToTimestampArray(List<object?> values, TimestampType timestampType)
        {
            var builder = new TimestampArray.Builder(timestampType);
            var epoch = new DateTimeOffset(1970, 1, 1, 0, 0, 0, TimeSpan.Zero);
            
            foreach (var value in values)
            {
                if (value == null)
                {
                    builder.AppendNull();
                }
                else
                {
                    DateTimeOffset dateTime;
                    if (value is DateTimeOffset dto)
                        dateTime = dto;
                    else if (value is DateTime dt)
                        dateTime = new DateTimeOffset(dt);
                    else
                        dateTime = DateTimeOffset.Parse(value.ToString()!);
                    
                    builder.Append(dateTime);
                }
            }
            return builder.Build();
        }

        private Decimal128Array ConvertToDecimal128Array(List<object?> values, Decimal128Type decimalType)
        {
            var builder = new Decimal128Array.Builder(decimalType);
            
            foreach (var value in values)
            {
                if (value == null)
                {
                    builder.AppendNull();
                }
                else
                {
                    var decimalValue = Convert.ToDecimal(value);
                    builder.Append(decimalValue);
                }
            }
            return builder.Build();
        }

        private BinaryArray ConvertToFixedSizeBinaryArray(List<object?> values, FixedSizeBinaryType fixedBinaryType)
        {
            // Apache Arrow doesn't have a specific FixedSizeBinaryArray, use BinaryArray
            var builder = new BinaryArray.Builder();
            
            foreach (var value in values)
            {
                if (value == null)
                {
                    builder.AppendNull();
                }
                else if (value is byte[] bytes)
                {
                    if (bytes.Length != fixedBinaryType.ByteWidth)
                    {
                        // Pad or truncate to match fixed size
                        var fixedBytes = new byte[fixedBinaryType.ByteWidth];
                        Buffer.BlockCopy(bytes, 0, fixedBytes, 0, Math.Min(bytes.Length, fixedBinaryType.ByteWidth));
                        builder.Append(fixedBytes.AsSpan());
                    }
                    else
                    {
                        builder.Append(bytes.AsSpan());
                    }
                }
                else if (value is Guid guid)
                {
                    // Special case for UUID (16 bytes)
                    builder.Append(guid.ToByteArray().AsSpan());
                }
                else
                {
                    throw new InvalidCastException($"Cannot convert {value.GetType()} to fixed size binary");
                }
            }
            return builder.Build();
        }
    }
}