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
using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Apache.Arrow.Adbc.Telemetry.Traces.Listeners.FileListener
{
    /// <summary>
    /// Writes OTel-compatible attribute values as native JSON. The OpenTelemetry spec allows
    /// attribute values to be <c>string</c>, <c>bool</c>, <c>long</c>, or <c>double</c>, or
    /// homogeneous arrays of those types. Values of those types are emitted as native JSON
    /// scalars/arrays. Anything else (DateTime, custom types, etc.) falls through to an
    /// invariant-culture string representation so the serialized form doesn't vary by locale.
    /// </summary>
    internal static class OtelAttributeWriter
    {
        public static void WriteValue(Utf8JsonWriter writer, object? value)
        {
            switch (value)
            {
                case null:
                    writer.WriteNullValue();
                    return;
                case string s:
                    writer.WriteStringValue(s);
                    return;
                case bool b:
                    writer.WriteBooleanValue(b);
                    return;
                // OTel's integer attributes are int64. Any integral type up to long fits.
                case long l:
                    writer.WriteNumberValue(l);
                    return;
                case int i:
                    writer.WriteNumberValue(i);
                    return;
                case short sh:
                    writer.WriteNumberValue(sh);
                    return;
                case byte by:
                    writer.WriteNumberValue(by);
                    return;
                case sbyte sb:
                    writer.WriteNumberValue(sb);
                    return;
                case uint ui:
                    writer.WriteNumberValue(ui);
                    return;
                case ulong ul:
                    writer.WriteNumberValue(ul);
                    return;
                case ushort us:
                    writer.WriteNumberValue(us);
                    return;
                // OTel floating-point is double; float widens without loss.
                case double d:
                    writer.WriteNumberValue(d);
                    return;
                case float f:
                    writer.WriteNumberValue(f);
                    return;
                // Homogeneous OTel arrays. Iterate with the concrete element type so each
                // scalar takes the native-JSON path rather than the fallback.
                case string?[] ss:
                    WriteArray(writer, ss);
                    return;
                case bool[] bs:
                    WriteArray(writer, bs);
                    return;
                case long[] ls:
                    WriteArray(writer, ls);
                    return;
                case int[] iarr:
                    WriteArray(writer, iarr);
                    return;
                case double[] ds:
                    WriteArray(writer, ds);
                    return;
                // Non-OTel types: invariant-culture string. This keeps the output
                // portable across locales for things like DateTime or custom structs.
                case IFormattable formattable:
                    writer.WriteStringValue(formattable.ToString(null, CultureInfo.InvariantCulture));
                    return;
                // Generic enumerable fallback (covers ImmutableArray, List<T>, etc.) —
                // each element is dispatched recursively.
                case IEnumerable enumerable:
                    writer.WriteStartArray();
                    foreach (object? item in enumerable)
                    {
                        WriteValue(writer, item);
                    }
                    writer.WriteEndArray();
                    return;
                default:
                    writer.WriteStringValue(value.ToString());
                    return;
            }
        }

        private static void WriteArray(Utf8JsonWriter writer, string?[] array)
        {
            writer.WriteStartArray();
            foreach (string? item in array)
            {
                if (item == null) { writer.WriteNullValue(); } else { writer.WriteStringValue(item); }
            }
            writer.WriteEndArray();
        }

        private static void WriteArray(Utf8JsonWriter writer, bool[] array)
        {
            writer.WriteStartArray();
            foreach (bool item in array) { writer.WriteBooleanValue(item); }
            writer.WriteEndArray();
        }

        private static void WriteArray(Utf8JsonWriter writer, long[] array)
        {
            writer.WriteStartArray();
            foreach (long item in array) { writer.WriteNumberValue(item); }
            writer.WriteEndArray();
        }

        private static void WriteArray(Utf8JsonWriter writer, int[] array)
        {
            writer.WriteStartArray();
            foreach (int item in array) { writer.WriteNumberValue(item); }
            writer.WriteEndArray();
        }

        private static void WriteArray(Utf8JsonWriter writer, double[] array)
        {
            writer.WriteStartArray();
            foreach (double item in array) { writer.WriteNumberValue(item); }
            writer.WriteEndArray();
        }
    }

    /// <summary>
    /// Write-only converter for <see cref="SerializableActivity.TagObjects"/> and similar
    /// <c>IReadOnlyDictionary&lt;string, object?&gt;</c> properties. Emits a JSON object.
    /// </summary>
    internal sealed class OtelAttributesDictionaryConverter : JsonConverter<IReadOnlyDictionary<string, object?>>
    {
        public override IReadOnlyDictionary<string, object?>? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
            => throw new NotSupportedException("Reading SerializableActivity from JSON is not supported.");

        public override void Write(Utf8JsonWriter writer, IReadOnlyDictionary<string, object?> value, JsonSerializerOptions options)
        {
            writer.WriteStartObject();
            foreach (KeyValuePair<string, object?> kv in value)
            {
                writer.WritePropertyName(kv.Key);
                OtelAttributeWriter.WriteValue(writer, kv.Value);
            }
            writer.WriteEndObject();
        }
    }

    /// <summary>
    /// Write-only converter for <c>IReadOnlyList&lt;KeyValuePair&lt;string, object?&gt;&gt;</c>
    /// properties (ActivityEvent.Tags, ActivityLink.Tags). Emits a JSON array of
    /// <c>{"Key": ..., "Value": ...}</c> objects to match the shape produced by the
    /// previous reflection-based JsonSerializer.
    /// </summary>
    internal sealed class OtelAttributesListConverter : JsonConverter<IReadOnlyList<KeyValuePair<string, object?>>>
    {
        public override IReadOnlyList<KeyValuePair<string, object?>>? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
            => throw new NotSupportedException("Reading SerializableActivity from JSON is not supported.");

        public override void Write(Utf8JsonWriter writer, IReadOnlyList<KeyValuePair<string, object?>> value, JsonSerializerOptions options)
        {
            writer.WriteStartArray();
            foreach (KeyValuePair<string, object?> kv in value)
            {
                writer.WriteStartObject();
                writer.WriteString("Key", kv.Key);
                writer.WritePropertyName("Value");
                OtelAttributeWriter.WriteValue(writer, kv.Value);
                writer.WriteEndObject();
            }
            writer.WriteEndArray();
        }
    }
}
