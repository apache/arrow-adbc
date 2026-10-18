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
using Apache.Arrow.Operations.Shredding;
using Apache.Arrow.Operations.VariantJson;
using Apache.Arrow.Scalars.Variant;

namespace Apache.Arrow.Adbc.Extensions
{
    /// <summary>
    /// Converts values of an <see cref="VariantArray"/> to JSON or .NET values.
    /// </summary>
    internal static class VariantConversions
    {
        private static readonly DateTimeOffset s_unixEpoch = new DateTimeOffset(1970, 1, 1, 0, 0, 0, TimeSpan.Zero);
        private static readonly DateTime s_unixEpochUnspecified = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Unspecified);

        public static object? GetValue(VariantArray array, int index, StructResultType resultType)
        {
            if (array.IsNull(index))
                return null;

            VariantValue value = array.IsShredded ? array.GetLogicalVariantValue(index) : array.GetVariantValue(index);
            return resultType == StructResultType.JsonString ? VariantJsonWriter.ToJson(value) : ToObject(value);
        }

        /// <summary>
        /// Converts a variant value to the equivalent .NET value. Objects become
        /// <see cref="Dictionary{TKey, TValue}"/> and arrays become <see cref="List{T}"/>.
        /// </summary>
        public static object? ToObject(VariantValue value)
        {
            if (value.IsObject)
            {
                Dictionary<string, object?> result = new Dictionary<string, object?>();
                foreach (KeyValuePair<string, VariantValue> field in value.AsObject())
                {
                    result.Add(field.Key, ToObject(field.Value));
                }
                return result;
            }

            if (value.IsArray)
            {
                IReadOnlyList<VariantValue> elements = value.AsArray();
                List<object?> result = new List<object?>(elements.Count);
                foreach (VariantValue element in elements)
                {
                    result.Add(ToObject(element));
                }
                return result;
            }

            switch (value.PrimitiveType)
            {
                case VariantPrimitiveType.NullType:
                    return null;
                case VariantPrimitiveType.BooleanTrue:
                case VariantPrimitiveType.BooleanFalse:
                    return value.AsBoolean();
                case VariantPrimitiveType.Int8:
                    return value.AsInt8();
                case VariantPrimitiveType.Int16:
                    return value.AsInt16();
                case VariantPrimitiveType.Int32:
                    return value.AsInt32();
                case VariantPrimitiveType.Int64:
                    return value.AsInt64();
                case VariantPrimitiveType.Float:
                    return value.AsFloat();
                case VariantPrimitiveType.Double:
                    return value.AsDouble();
                case VariantPrimitiveType.Decimal4:
                case VariantPrimitiveType.Decimal8:
                    return value.AsDecimal();
                case VariantPrimitiveType.Decimal16:
                    return value.AsSqlDecimal();
                case VariantPrimitiveType.Date:
                    return value.AsDate();
                case VariantPrimitiveType.Timestamp:
                    return value.AsTimestamp();
                case VariantPrimitiveType.TimestampNtz:
                    return value.AsTimestampNtz();
                case VariantPrimitiveType.TimestampTzNanos:
                    return s_unixEpoch.AddTicks(value.AsTimestampTzNanos() / 100);
                case VariantPrimitiveType.TimestampNtzNanos:
                    return s_unixEpochUnspecified.AddTicks(value.AsTimestampNtzNanos() / 100);
                case VariantPrimitiveType.TimeNtz:
#if NET6_0_OR_GREATER
                    return new TimeOnly(value.AsTimeNtzMicros() * 10);
#else
                    return TimeSpan.FromTicks(value.AsTimeNtzMicros() * 10);
#endif
                case VariantPrimitiveType.String:
                    return value.AsString();
                case VariantPrimitiveType.Binary:
                    return value.AsBinary();
                case VariantPrimitiveType.Uuid:
                    return value.AsUuid();
                default:
                    throw new NotSupportedException($"Unsupported variant type: {value.PrimitiveType}");
            }
        }
    }
}
