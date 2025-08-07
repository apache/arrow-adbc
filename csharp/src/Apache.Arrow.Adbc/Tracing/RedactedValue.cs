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
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Apache.Arrow.Adbc.Tracing
{
    [JsonConverter(typeof(RedactedValueConverter))]
    public struct RedactedValue
    {
        public const string RedactedValueDefault = "[redacted]";

        private readonly object? _unredactedValue;
        private readonly bool _isPii;

        /// <summary>
        /// Creates a new instance of <see cref="RedactedValue"/>.
        /// By default, the value is considered PII (Personally Identifiable Information).
        /// Use the constructor with the `isPii` parameter set to false if the value is not PII.
        /// </summary>
        /// <param name="value"></param>
        /// <param name="isPii"></param>
        public RedactedValue(object? value, bool isPii = true)
        {
            _unredactedValue = value;
            _isPii = isPii;
        }

        public readonly object? Value => _unredactedValue != null && _isPii ? RedactedValueDefault : _unredactedValue;

        public readonly bool GetIsPii() => _isPii;

        public readonly object? GetUnredactedValue() => _unredactedValue;

        public override readonly string? ToString() => Value?.ToString();

        public override readonly bool Equals(object? obj)
        {
            return obj is RedactedValue value
                && EqualityComparer<object?>.Default.Equals(_unredactedValue, value._unredactedValue)
                && _isPii == value._isPii;
        }

        public override readonly int GetHashCode()
        {
#if NET5_0_OR_GREATER
            return HashCode.Combine(_unredactedValue, _isPii);
#else
            return new { _unredactedValue, _isPii }.GetHashCode();
#endif
        }

        public static bool operator ==(RedactedValue left, RedactedValue right)
        {
            return left.Equals(right);
        }

        public static bool operator !=(RedactedValue left, RedactedValue right)
        {
            return !(left == right);
        }
    }

    internal class RedactedValueConverter : JsonConverter<RedactedValue>
    {
        public override RedactedValue Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            throw new NotImplementedException("RedactedValueConverter does not support reading from JSON. Use RedactedValue directly instead.");
        }

        public override void Write(Utf8JsonWriter writer, RedactedValue value, JsonSerializerOptions options)
        {
            // Writes the redacted value without (Value) property name.
            writer.WriteRawValue(JsonSerializer.SerializeToUtf8Bytes(value.Value, options));
        }
    }
}
